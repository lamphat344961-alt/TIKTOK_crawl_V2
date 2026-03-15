"""
db/db_manager.py
================
Quản lý kết nối và thao tác với SQL Server theo schema TikTok_Creator.
Nguồn creators list vẫn lấy từ MongoDB ở main.py; file này chỉ làm việc với SQL Server.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime

import pyodbc

from helpers import make_comment_id, make_reply_id, parse_count


class DBManager:
    """SQL Server manager cho creators, tags, videos, comments, replies."""

    def __init__(self):
        self.conn = None
        self.cursor = None
        self._table_columns_cache: dict[str, set[str]] = {}
        self._connect()

    # ------------------------------------------------------------------
    # KẾT NỐI
    # ------------------------------------------------------------------

    def _build_conn_str(self) -> str:
        driver   = os.getenv("SQL_DRIVER",   os.getenv("SQL_SERVER_DRIVER",   "ODBC Driver 17 for SQL Server"))
        server   = os.getenv("SQL_SERVER",   os.getenv("SQL_SERVER_HOST",     r"ACER-TANPHAT\SQLEXPRESS"))
        database = os.getenv("SQL_DATABASE", os.getenv("SQL_SERVER_DATABASE", "TikTok_Creator"))
        trusted  = os.getenv("SQL_SERVER_TRUSTED_CONNECTION", "1").strip().lower() in {"1", "true", "yes", "y"}
        username = os.getenv("SQL_USERNAME", os.getenv("SQL_SERVER_USERNAME", ""))
        password = os.getenv("SQL_PASSWORD", os.getenv("SQL_SERVER_PASSWORD", ""))

        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={server}",
            f"DATABASE={database}",
        ]
        if trusted:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={username}")
            parts.append(f"PWD={password}")
        return ";".join(parts) + ";"

    def _connect(self):
        try:
            self.conn   = pyodbc.connect(self._build_conn_str(), timeout=10)
            self.cursor = self.conn.cursor()
            self.cursor.execute("SELECT DB_NAME()")
            db_name = self.cursor.fetchone()[0]
            print(f"[db] Kết nối SQL Server thành công: {db_name}")
        except Exception as e:
            print(f"[db] Lỗi kết nối SQL Server: {e}")
            self.conn   = None
            self.cursor = None

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            print("[db] Đã đóng kết nối SQL Server")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # SCHEMA INTROSPECTION
    # ------------------------------------------------------------------

    def _table_columns(self, table_name: str) -> set[str]:
        """
        Trả về tập hợp tên cột (UPPERCASE) của bảng.
        FIX: Query với TABLE_NAME theo đúng case thật trong DB thay vì uppercase,
        đồng thời upper() kết quả để so sánh nhất quán.
        """
        key = table_name.upper()
        if key in self._table_columns_cache:
            return self._table_columns_cache[key]

        cols: set[str] = set()
        if not self.cursor:
            return cols

        try:
            # Dùng UPPER() ở phía SQL để không phụ thuộc collation
            self.cursor.execute(
                """
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE UPPER(TABLE_NAME) = ?
                """,
                key,
            )
            cols = {row[0] for row in self.cursor.fetchall()}
        except Exception as e:
            print(f"[db] Lỗi đọc schema bảng {key}: {e}")
        self._table_columns_cache[key] = cols
        return cols

    # ------------------------------------------------------------------
    # PARSE HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _to_int(value):
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            parsed = parse_count(s)
            if parsed is not None:
                return parsed
            digits = re.sub(r"[^\d-]", "", s)
            if digits in {"", "-"}:
                return None
            try:
                return int(digits)
            except ValueError:
                return None
        return None

    @staticmethod
    def _to_float(value):
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            s = value.strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                try:
                    return float(s[:-1]) / 100.0
                except ValueError:
                    return None
            try:
                return float(s)
            except ValueError:
                return None
        return None

    @staticmethod
    def _to_datetime(value):
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value)
            except Exception:
                return None
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            if s.isdigit():
                try:
                    return datetime.fromtimestamp(int(s))
                except Exception:
                    return None
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
        return None

    @staticmethod
    def _to_bit(value):
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)):
            return 1 if value else 0
        if isinstance(value, str):
            s = value.strip().lower()
            if s in {"1", "true", "yes", "y"}:
                return 1
            if s in {"0", "false", "no", "n"}:
                return 0
        return None

    @staticmethod
    def _normalize_tags(raw_tags) -> list[str]:
        if raw_tags is None:
            return []
        if isinstance(raw_tags, list):
            items = raw_tags
        else:
            text = str(raw_tags).strip()
            if not text:
                return []
            items = re.split(r"[|,;/]+", text)
        seen = set()
        out  = []
        for item in items:
            tag = str(item).strip()
            if not tag:
                continue
            key = tag.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(tag)
        return out

    @staticmethod
    def _label_texts(label_list) -> str | None:
        if not label_list:
            return None
        texts = []
        for item in label_list:
            if isinstance(item, dict):
                text = str(item.get("text") or "").strip()
                if text:
                    texts.append(text)
        return "|".join(texts) if texts else None

    @staticmethod
    def _to_raw_json(obj) -> str | None:
        """Serialize raw API object thành JSON string để lưu vào RAW_JSON."""
        if obj is None:
            return None
        try:
            return json.dumps(obj, ensure_ascii=False)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # GENERIC UPSERT HELPERS
    # ------------------------------------------------------------------

    def _upsert_simple(
        self,
        table: str,
        key_cols: list[str],
        row: dict,
    ):
        if not self.cursor:
            return
        cols = [c for c, v in row.items() if v is not None]
        if not cols:
            return

        non_keys   = [c for c in cols if c not in key_cols]
        on_clause  = " AND ".join([f"target.[{c}] = src.[{c}]" for c in key_cols])
        src_cols   = ", ".join([f"? AS [{c}]" for c in cols])
        insert_cols = ", ".join([f"[{c}]" for c in cols])
        insert_vals = ", ".join([f"src.[{c}]" for c in cols])

        sql = [
            f"MERGE [{table}] AS target",
            f"USING (SELECT {src_cols}) AS src",
            f"ON {on_clause}",
        ]
        if non_keys:
            set_clause = ", ".join([f"target.[{c}] = src.[{c}]" for c in non_keys])
            sql.append(f"WHEN MATCHED THEN UPDATE SET {set_clause}")
        sql.append(f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});")
        params = [row[c] for c in cols]
        self.cursor.execute("\n".join(sql), params)

    # ------------------------------------------------------------------
    # CREATORS / TAGS
    # ------------------------------------------------------------------

    def upsert_creator(self, creator_id: str, source_doc: dict, profile_stats: dict):
        if not self.cursor:
            return

        row = {
            "CREATOR_ID":         str(creator_id).strip(),
            "FOLLOWERS":          profile_stats.get("followers_count") if profile_stats.get("followers_count") is not None
                                  else self._to_int(source_doc.get("Followers")),
            "ENGAGEMENT":         self._to_float(source_doc.get("Engagement")),
            "MEDIAN_VIEWS":       self._to_int(source_doc.get("Median Views")),
            "TOTAL_LIKES":        profile_stats.get("total_likes"),
            "COLLAB_SCORE":       self._to_float(source_doc.get("Collab Score")),
            "PRICE":              self._to_float(source_doc.get("Start Price")),
            "MISSING_PRICE_FLAG": 0 if self._to_float(source_doc.get("Start Price")) is not None else 1,
            "SNAPSHOT_TIME":      datetime.now(),
        }

        try:
            self._upsert_simple("CREATORS", ["CREATOR_ID"], row)
            self.conn.commit()
            print(f"[db] Upsert creator: {creator_id}")
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi upsert creator {creator_id}: {e}")

    def sync_creator_tags(self, creator_id: str, raw_tags):
        if not self.cursor:
            return

        tags = self._normalize_tags(raw_tags)
        try:
            desired_tag_ids = []
            for tag_name in tags:
                self.cursor.execute(
                    """
                    MERGE [TAGS] AS target
                    USING (SELECT ? AS [TAG_NAME]) AS src
                    ON target.[TAG_NAME] = src.[TAG_NAME]
                    WHEN NOT MATCHED THEN
                        INSERT ([TAG_NAME]) VALUES (src.[TAG_NAME]);
                    """,
                    tag_name,
                )
                self.cursor.execute("SELECT [TAG_ID] FROM [TAGS] WHERE [TAG_NAME] = ?", tag_name)
                row = self.cursor.fetchone()
                if row:
                    desired_tag_ids.append(int(row[0]))

            self.cursor.execute("DELETE FROM [CREATOR_TAGS] WHERE [CREATOR_ID] = ?", creator_id)
            for tag_id in desired_tag_ids:
                self.cursor.execute(
                    """
                    INSERT INTO [CREATOR_TAGS] ([CREATOR_ID], [TAG_ID])
                    VALUES (?, ?)
                    """,
                    creator_id,
                    tag_id,
                )

            self.conn.commit()
            print(f"[db] Sync tags creator {creator_id}: {len(desired_tag_ids)} tags")
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi sync tags {creator_id}: {e}")

    # ------------------------------------------------------------------
    # VIDEOS
    # ------------------------------------------------------------------

    def upsert_video(self, creator_id: str, stats: dict):
        if not self.cursor:
            return
        video_id = stats.get("video_id")
        if not video_id:
            return

        row = {
            "VIDEO_ID":      str(video_id),
            "CREATOR_ID":    str(creator_id),
            "CREATE_TIME":   self._to_datetime(stats.get("create_time")),
            "VIEW_COUNT":    self._to_int(stats.get("view_count")),
            "LIKE_COUNT":    self._to_int(stats.get("like_count")),
            "COMMENT_COUNT": self._to_int(stats.get("comment_count")),
            "SAVE_COUNT":    self._to_int(stats.get("save_count_ui")),
            "SNAPSHOT_TIME": datetime.now(),
        }
        if stats.get("share_count") is not None:
            row["SHARE_COUNT"] = self._to_int(stats.get("share_count"))

        try:
            self._upsert_simple("VIDEOS", ["VIDEO_ID", "CREATOR_ID"], row)
            self.conn.commit()
            print(f"[db] Upsert video: {video_id}")
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi upsert video {video_id}: {e}")

    # ------------------------------------------------------------------
    # DUPLICATE DETECTION
    # ------------------------------------------------------------------

    def get_existing_comment_cids(self, video_id: str, creator_id: str | None = None) -> set[str]:
        if not self.cursor:
            return set()
        try:
            if creator_id is None:
                self.cursor.execute(
                    "SELECT [COMMENT_ID] FROM [COMMENTS] WHERE [VIDEO_ID] = ?",
                    str(video_id),
                )
            else:
                self.cursor.execute(
                    "SELECT [COMMENT_ID] FROM [COMMENTS] WHERE [VIDEO_ID] = ? AND [CREATOR_ID] = ?",
                    str(video_id),
                    str(creator_id),
                )
            return {str(row[0]) for row in self.cursor.fetchall() if row[0] is not None}
        except Exception as e:
            print(f"[db] Lỗi get_existing_comment_cids {video_id}: {e}")
            return set()

    def get_existing_reply_cids_for_comment(
        self,
        comment_id: str,
        video_id: str | None = None,
        creator_id: str | None = None,
    ) -> set[str]:
        if not self.cursor:
            return set()
        try:
            if video_id is not None and creator_id is not None:
                self.cursor.execute(
                    """
                    SELECT [REPLY_ID]
                    FROM [REPLIES]
                    WHERE [PARENT_CMT_ID] = ? AND [VIDEO_ID] = ? AND [CREATOR_ID] = ?
                    """,
                    str(comment_id),
                    str(video_id),
                    str(creator_id),
                )
            else:
                self.cursor.execute(
                    "SELECT [REPLY_ID] FROM [REPLIES] WHERE [PARENT_CMT_ID] = ?",
                    str(comment_id),
                )
            return {str(row[0]) for row in self.cursor.fetchall() if row[0] is not None}
        except Exception as e:
            print(f"[db] Lỗi get_existing_reply_cids_for_comment {comment_id}: {e}")
            return set()

    # ------------------------------------------------------------------
    # COMMENTS
    # ------------------------------------------------------------------

    def upsert_comments(
        self,
        comments: list[dict],
        creator_id: str,
        video_id: str,
        skip_cids: set | None = None,
    ) -> int:
        if not self.cursor or not comments:
            return 0

        skip_cids = skip_cids or set()
        available = self._table_columns("COMMENTS")
        saved     = 0

        try:
            for c in comments:
                cid = c.get("cid")
                comment_id = (
                    str(cid) if cid
                    else make_comment_id(
                        str(video_id),
                        str(c.get("create_time") or ""),
                        c.get("text") or "",
                    )
                )
                if comment_id in skip_cids:
                    continue

                # --- core fields (luôn lưu) ---
                row: dict = {
                    "COMMENT_ID":    comment_id,
                    "VIDEO_ID":      str(video_id),
                    "CREATOR_ID":    str(creator_id),
                    "COMMENT_TIME":  self._to_datetime(c.get("create_time")),
                    "LIKE_COUNT":    self._to_int(c.get("digg_count")),
                    "REPLY_COUNT":   self._to_int(c.get("reply_comment_total")),
                    "TEXT":          c.get("text"),
                    "SNAPSHOT_TIME": datetime.now(),
                }

                # --- optional fields (lưu nếu cột tồn tại trong schema) ---
                optional: dict = {
                    "CREATE_TIME_TS":           self._to_int(c.get("create_time")),
                    "COMMENT_LANGUAGE":         c.get("comment_language"),
                    "IS_HIGH_PURCHASE_INTENT":  self._to_bit(c.get("is_high_purchase_intent")),
                    "CUSTOM_VERIFY":            c.get("custom_verify"),
                    "FOLD_STATUS":              self._to_int(c.get("fold_status")),
                    "IS_AUTHOR_DIGGED":         self._to_bit(c.get("is_author_digged")),
                    "LABEL_TEXTS":              self._label_texts(c.get("label_list")),
                    "NO_SHOW":                  self._to_bit(c.get("no_show")),
                    "ENTERPRISE_VERIFY_REASON": c.get("enterprise_verify_reason"),
                    # FIX: lưu toàn bộ raw JSON để không mất field nào
                    "RAW_JSON":                 self._to_raw_json(c),
                }
                for col, val in optional.items():
                    if col in available:
                        row[col] = val

                self._upsert_simple("COMMENTS", ["COMMENT_ID", "VIDEO_ID", "CREATOR_ID"], row)
                saved += 1

            self.conn.commit()
            print(f"[db] Upsert comments: {saved} rows (video {video_id})")
            return saved
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi upsert comments video {video_id}: {e}")
            return 0

    # ------------------------------------------------------------------
    # REPLIES
    # ------------------------------------------------------------------

    def upsert_replies(
        self,
        replies: list[dict],
        creator_id: str,
        video_id: str,
        parent_comment_id: str,
        skip_cids: set | None = None,
    ) -> int:
        if not self.cursor or not replies:
            return 0

        skip_cids = skip_cids or set()
        # FIX: dùng _table_columns() để guard đúng như upsert_comments
        available = self._table_columns("REPLIES")
        saved     = 0

        try:
            for r in replies:
                cid = r.get("cid")
                reply_id = (
                    str(cid) if cid
                    else make_reply_id(
                        str(parent_comment_id),
                        str(r.get("create_time") or ""),
                        r.get("text") or "",
                    )
                )
                if reply_id in skip_cids:
                    continue

                # --- core fields (luôn lưu) ---
                row: dict = {
                    "REPLY_ID":       reply_id,
                    "PARENT_CMT_ID":  str(parent_comment_id),
                    "VIDEO_ID":       str(video_id),
                    "CREATOR_ID":     str(creator_id),
                    "REPLY_TIME":     self._to_datetime(r.get("create_time")),
                    "LIKE_COUNT":     self._to_int(r.get("digg_count")),
                    "TEXT":           r.get("text"),
                    "SNAPSHOT_TIME":  datetime.now(),
                }

                # --- optional fields (lưu nếu cột tồn tại trong schema) ---
                # FIX: bổ sung đầy đủ tất cả optional fields giống COMMENTS
                optional: dict = {
                    "CREATE_TIME_TS":           self._to_int(r.get("create_time")),
                    "REPLY_LANGUAGE":           r.get("comment_language"),   # API trả về key "comment_language" cho cả reply
                    "IS_HIGH_PURCHASE_INTENT":  self._to_bit(r.get("is_high_purchase_intent")),
                    "CUSTOM_VERIFY":            r.get("custom_verify"),
                    "FOLD_STATUS":              self._to_int(r.get("fold_status")),
                    "IS_AUTHOR_DIGGED":         self._to_bit(r.get("is_author_digged")),
                    "LABEL_TEXTS":              self._label_texts(r.get("label_list")),
                    "NO_SHOW":                  self._to_bit(r.get("no_show")),
                    "ENTERPRISE_VERIFY_REASON": r.get("enterprise_verify_reason"),
                    # FIX: lưu toàn bộ raw JSON để không mất field nào
                    "RAW_JSON":                 self._to_raw_json(r),
                }
                for col, val in optional.items():
                    if col in available:
                        row[col] = val

                self._upsert_simple(
                    "REPLIES",
                    ["REPLY_ID", "PARENT_CMT_ID", "VIDEO_ID", "CREATOR_ID"],
                    row,
                )
                saved += 1

            self.conn.commit()
            print(f"[db] Upsert replies: {saved} rows (comment {parent_comment_id})")
            return saved
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi upsert replies comment {parent_comment_id}: {e}")
            return 0