"""
db/db_manager.py
================
Quản lý kết nối và thao tác với SQL Server theo schema TikTok_Creator.

Thay đổi so với phiên bản cũ:
  - upsert_creator()       : nhận dict UPPERCASE từ ProfileFeedCrawler
  - upsert_video()         : nhận dict UPPERCASE từ ProfileFeedCrawler
  - load_creator_inputs()  : đọc danh sách creator từ SQL Server (bảng CREATORS),
                             thay thế MongoDB; hỗ trợ MAX_CREATORS
  - get_crawled_creator_ids(): phát hiện creator đã crawl để skip (checkpoint)
  - _ensure_connection()   : auto-reconnect khi kết nối bị drop sau nhiều giờ
  - ANCHOR_TYPES           : list Python → json.dumps tự động
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime

import pyodbc

from crawler.helpers import make_comment_id, make_reply_id, parse_count


class DBManager:
    """SQL Server manager cho creators, tags, videos, comments, replies."""

    def __init__(self):
        self.conn   = None
        self.cursor = None
        self._table_columns_cache: dict[str, set[str]] = {}
        self._conn_str = self._build_conn_str()
        self._connect()

    # ------------------------------------------------------------------
    # KẾT NỐI & RECONNECT
    # ------------------------------------------------------------------

    def _build_conn_str(self) -> str:
        driver   = os.getenv("SQL_DRIVER",   os.getenv("SQL_SERVER_DRIVER",   "ODBC Driver 17 for SQL Server"))
        server   = os.getenv("SQL_SERVER",   os.getenv("SQL_SERVER_HOST",     r"ACER-TANPHAT\SQLEXPRESS"))
        database = os.getenv("SQL_DATABASE", os.getenv("SQL_SERVER_DATABASE", "TikTok_Creator_DB_v1"))
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
            self.conn   = pyodbc.connect(self._conn_str, timeout=10)
            self.cursor = self.conn.cursor()
            self.cursor.execute("SELECT DB_NAME()")
            db_name = self.cursor.fetchone()[0]
            print(f"[db] Kết nối SQL Server thành công: {db_name}")
        except Exception as e:
            print(f"[db] Lỗi kết nối SQL Server: {e}")
            self.conn   = None
            self.cursor = None

    def _ensure_connection(self):
        """
        Kiểm tra kết nối còn sống không; nếu chết thì reconnect.
        Gọi trước mỗi thao tác DB quan trọng để tránh lỗi âm thầm
        sau nhiều giờ crawl liên tục.
        """
        try:
            if self.cursor:
                self.cursor.execute("SELECT 1")
                return  # vẫn sống
        except Exception:
            pass
        print("[db] Kết nối bị drop, đang reconnect...")
        self._table_columns_cache.clear()  # schema cache có thể lỗi thời
        self._connect()

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
        key = table_name.upper()
        if key in self._table_columns_cache:
            return self._table_columns_cache[key]

        cols: set[str] = set()
        if not self.cursor:
            return cols

        try:
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
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
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
        if obj is None:
            return None
        if isinstance(obj, str):
            return obj  # đã là JSON string rồi
        try:
            return json.dumps(obj, ensure_ascii=False)
        except Exception:
            return None

    @staticmethod
    def _anchor_types_to_str(value) -> str | None:
        """
        ANCHOR_TYPES từ ProfileFeedCrawler đã là json string.
        Nếu vì lý do nào đó vẫn là list thì serialize lại.
        """
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return None
        return str(value)

    # ------------------------------------------------------------------
    # GENERIC UPSERT HELPER
    # ------------------------------------------------------------------

    def _upsert_simple(self, table: str, key_cols: list[str], row: dict):
        if not self.cursor:
            return
        cols = [c for c, v in row.items() if v is not None]
        if not cols:
            return

        non_keys    = [c for c in cols if c not in key_cols]
        on_clause   = " AND ".join([f"target.[{c}] = src.[{c}]" for c in key_cols])
        src_cols    = ", ".join([f"? AS [{c}]" for c in cols])
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
    # LOAD CREATOR INPUTS
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # KIỂM TRA CREATOR_ID HỢP LỆ
    # ------------------------------------------------------------------

    @staticmethod
    def _is_numeric_tiktok_id(creator_id: str) -> bool:
        """
        Trả về True nếu creator_id trông như numeric TikTok ID (toàn chữ số, dài >= 15 ký tự).
        CREATOR_ID đúng phải là username (ví dụ: "banhsukem", "lethikhanhhuyen2004").
        Numeric ID (ví dụ: "6716730587635123") là dữ liệu bị import sai schema.
        """
        return creator_id.isdigit() and len(creator_id) >= 15

    def audit_creator_id_schema(self):
        """
        Kiểm tra và in ra các CREATOR_ID trong bảng CREATORS có dạng numeric
        (dấu hiệu bị import sai — nên là username TikTok).
        Gọi thủ công khi debug, không ảnh hưởng luồng chính.
        """
        self._ensure_connection()
        if not self.cursor:
            return
        try:
            self.cursor.execute("SELECT [CREATOR_ID] FROM [CREATORS]")
            all_ids = [str(r[0]).strip() for r in self.cursor.fetchall() if r[0]]
            bad = [cid for cid in all_ids if self._is_numeric_tiktok_id(cid)]
            if bad:
                print(f"[db] AUDIT: Tìm thấy {len(bad)} CREATOR_ID dạng numeric (import sai):")
                for cid in bad[:20]:
                    print(f"         {cid}")
                if len(bad) > 20:
                    print(f"         ... và {len(bad) - 20} ID khác")
                print("[db] AUDIT: Cần chạy script chuẩn hóa CREATOR_ID thành username.")
            else:
                print(f"[db] AUDIT: Tất cả {len(all_ids)} CREATOR_ID đều hợp lệ (dạng username).")
        except Exception as e:
            print(f"[db] Lỗi audit_creator_id_schema: {e}")

    # ------------------------------------------------------------------
    # LOAD CREATOR INPUTS
    # ------------------------------------------------------------------

    def load_creator_inputs(self) -> list[dict]:
        """
        Đọc danh sách creator cần crawl từ SQL Server.

        Chỉ lấy creator có CRAWL_STATUS IN ('pending', 'in_progress', 'error'):
          - 'pending'     : chưa crawl lần nào
          - 'in_progress' : đã bắt đầu nhưng bị crash giữa chừng → crawl lại
          - 'done'        : đã crawl đủ 90 ngày → bỏ qua

        Hỗ trợ MAX_CREATORS: nếu config.MAX_CREATORS không None thì giới hạn số lượng.

        BUG FIX: Lọc ra và cảnh báo các CREATOR_ID dạng numeric (import sai schema).
        CREATOR_ID phải là username TikTok (chuỗi chữ) để khớp với FK chain
        CREATORS → VIDEOS → COMMENTS → REPLIES.
        """
        import crawler.config as cfg
        self._ensure_connection()

        max_c = getattr(cfg, "MAX_CREATORS", None)
        rows  = []

        if not self.cursor:
            print("[db] ERROR: Không có kết nối DB, không thể đọc danh sách creator.")
            return rows

        try:
            self.cursor.execute(
                """
                SELECT c.[CREATOR_ID],
                       c.[CRAWL_STATUS],
                       STRING_AGG(t.[TAG_NAME], '|') AS TAGS
                FROM [CREATORS] c
                LEFT JOIN [CREATOR_TAGS] ct ON ct.[CREATOR_ID] = c.[CREATOR_ID]
                LEFT JOIN [TAGS] t           ON t.[TAG_ID]     = ct.[TAG_ID]
                WHERE c.[CRAWL_STATUS] IN ('pending', 'in_progress', 'error')
                GROUP BY c.[CREATOR_ID], c.[CRAWL_STATUS]
                ORDER BY c.[CREATOR_ID]
                """
            )
            db_rows = self.cursor.fetchall()
            skipped_numeric = []
            for r in db_rows:
                creator_id   = str(r[0]).strip() if r[0] else None
                crawl_status = str(r[1]).strip() if r[1] else "pending"
                tags_raw     = str(r[2]).strip() if r[2] else ""
                if not creator_id:
                    continue

                # BUG FIX: Bỏ qua CREATOR_ID dạng numeric — đây là dữ liệu import sai.
                # Nếu CREATOR_ID là số (ví dụ: "6716730587635123"), video/comment sẽ KHÔNG
                # thể insert vì crawler luôn dùng username làm CREATOR_ID → FK bị vi phạm.
                if self._is_numeric_tiktok_id(creator_id):
                    skipped_numeric.append(creator_id)
                    continue

                rows.append({
                    "ID":           creator_id,
                    "CRAWL_STATUS": crawl_status,
                    "Tags":         [t for t in tags_raw.split("|") if t] if tags_raw else [],
                })

            if skipped_numeric:
                print(
                    f"[db] WARNING: Bỏ qua {len(skipped_numeric)} CREATOR_ID dạng numeric "
                    f"(import sai schema). Ví dụ: {skipped_numeric[:3]}"
                )
                print("[db] ACTION REQUIRED: Cần chuẩn hóa CREATOR_ID thành username TikTok "
                      "trong bảng CREATORS. Xem hàm audit_creator_id_schema() để biết thêm.")

            done_count = self._count_creators_by_status("done")
            print(
                f"[db] load_creator_inputs: {len(rows)} creators hợp lệ cần crawl "
                f"(pending/in_progress/error) | {done_count} đã done"
            )
        except Exception as e:
            print(f"[db] Lỗi đọc CREATORS từ SQL: {e}")

        if not rows:
            print("[db] Không còn creator nào cần crawl "
                  "(tất cả đã 'done' hoặc bảng CREATORS trống).")

        # Áp dụng giới hạn
        if max_c is not None and max_c > 0:
            rows = rows[:max_c]
            print(f"[db] Giới hạn MAX_CREATORS={max_c} → {len(rows)} creators")

        return rows

    def _count_creators_by_status(self, status: str) -> int:
        """Đếm số creator theo CRAWL_STATUS."""
        try:
            self.cursor.execute(
                "SELECT COUNT(*) FROM [CREATORS] WHERE [CRAWL_STATUS] = ?", status
            )
            row = self.cursor.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def set_crawl_status(self, creator_id: str, status: str):
        """
        Cập nhật CRAWL_STATUS cho 1 creator.
        status: 'pending' | 'in_progress' | 'done' | 'not_found' | 'private' | 'banned' | 'no_videos' | 'error'
        """
        self._ensure_connection()
        if not self.cursor:
            return
        try:
            self.cursor.execute(
                "UPDATE [CREATORS] SET [CRAWL_STATUS] = ? WHERE [CREATOR_ID] = ?",
                status,
                str(creator_id),
            )
            self.conn.commit()
            print(f"[db] CRAWL_STATUS={status} cho creator {creator_id}")
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi set_crawl_status {creator_id}: {e}")

    # ------------------------------------------------------------------
    # CREATORS / TAGS
    # ------------------------------------------------------------------

    def upsert_creator(self, creator_dict: dict):
        """
        Ghi thông tin creator vào bảng CREATORS.

        Nhận dict UPPERCASE từ ProfileFeedCrawler._build_creator_dict().
        Các key quan trọng:
          CREATOR_ID, FOLLOWERS, FOLLOWING_COUNT, FRIEND_COUNT,
          TOTAL_LIKES, DIGG_COUNT, VIDEO_COUNT,
          ENGAGEMENT, MEDIAN_VIEWS, SNAPSHOT_TIME, RAW_JSON

        Nếu source_doc cũ (MongoDB) cần merge thêm field như COLLAB_SCORE, PRICE,
        gọi upsert_creator_source() riêng.
        """
        self._ensure_connection()
        if not self.cursor:
            return

        creator_id = str(creator_dict.get("CREATOR_ID") or "").strip()
        if not creator_id:
            print("[db] upsert_creator: CREATOR_ID rỗng, bỏ qua")
            return

        row = {
            "CREATOR_ID":       creator_id,
            "FOLLOWERS":        self._to_int(creator_dict.get("FOLLOWERS")),
            "FOLLOWING_COUNT":  self._to_int(creator_dict.get("FOLLOWING_COUNT")),
            "FRIEND_COUNT":     self._to_int(creator_dict.get("FRIEND_COUNT")),
            "TOTAL_LIKES":      self._to_int(creator_dict.get("TOTAL_LIKES")),
            "DIGG_COUNT":       self._to_int(creator_dict.get("DIGG_COUNT")),
            "VIDEO_COUNT":      self._to_int(creator_dict.get("VIDEO_COUNT")),
            "ENGAGEMENT":       self._to_float(creator_dict.get("ENGAGEMENT")),
            "MEDIAN_VIEWS":     self._to_int(creator_dict.get("MEDIAN_VIEWS")),
            "SNAPSHOT_TIME":    creator_dict.get("SNAPSHOT_TIME") or datetime.now(),
            "RAW_JSON":         None , 
        }

        # Giữ COLLAB_SCORE / PRICE nếu đã có từ lần import trước
        # (không overwrite bằng NULL từ crawler mới)
        row = {k: v for k, v in row.items() if v is not None}

        try:
            self._upsert_simple("CREATORS", ["CREATOR_ID"], row)
            self.conn.commit()
            print(f"[db] Upsert creator: {creator_id}")
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi upsert creator {creator_id}: {e}")

    def upsert_creator_source(self, creator_id: str, source_doc: dict):
        """
        Ghi các field phân tích (COLLAB_SCORE, PRICE, ENGAGEMENT, MEDIAN_VIEWS, CATEGORY)
        từ file Excel/CSV gốc vào bảng CREATORS.
        Tách riêng để không xung đột với upsert_creator() từ crawler.
        """
        self._ensure_connection()
        if not self.cursor:
            return

        row = {
            "CREATOR_ID":   str(creator_id).strip(),
            "ENGAGEMENT":   self._to_float(source_doc.get("Engagement")),
            "MEDIAN_VIEWS": self._to_int(source_doc.get("Median Views")),
            "COLLAB_SCORE": self._to_float(source_doc.get("Collab Score")),
            "PRICE":        self._to_float(source_doc.get("Start Price")),
            "CATEGORY":     source_doc.get("Category"),
            "MISSING_PRICE_FLAG": 0 if self._to_float(source_doc.get("Start Price")) is not None else 1,
            "SNAPSHOT_TIME": datetime.now(),
        }
        row = {k: v for k, v in row.items() if v is not None}

        try:
            self._upsert_simple("CREATORS", ["CREATOR_ID"], row)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"[db] Lỗi upsert_creator_source {creator_id}: {e}")

    def sync_creator_tags(self, creator_id: str, raw_tags):
        self._ensure_connection()
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
                    WHEN NOT MATCHED THEN INSERT ([TAG_NAME]) VALUES (src.[TAG_NAME]);
                    """,
                    tag_name,
                )
                self.cursor.execute("SELECT [TAG_ID] FROM [TAGS] WHERE [TAG_NAME] = ?", tag_name)
                row = self.cursor.fetchone()
                if row:
                    desired_tag_ids.append(int(row[0]))

            self.cursor.execute(
                "DELETE FROM [CREATOR_TAGS] WHERE [CREATOR_ID] = ?", creator_id
            )
            for tag_id in desired_tag_ids:
                self.cursor.execute(
                    "INSERT INTO [CREATOR_TAGS] ([CREATOR_ID], [TAG_ID]) VALUES (?, ?)",
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

    def upsert_video(self, video_dict: dict):
        """
        Ghi thông tin video vào bảng VIDEOS.

        Nhận dict UPPERCASE từ ProfileFeedCrawler._build_video_dict().
        Các key:
          VIDEO_ID, CREATOR_ID, CREATE_TIME, ANCHOR_TYPES,
          VIEW_COUNT, LIKE_COUNT, COMMENT_COUNT, SHARE_COUNT, SAVE_COUNT,
          VQSCORE, BITRATE, CATEGORY_TYPE, TITLE, DESC,
          MUSIC_TITLE, MUSIC_AUTHOR, SNAPSHOT_TIME, RAW_JSON
        """
        self._ensure_connection()
        if not self.cursor:
            return

        video_id   = str(video_dict.get("VIDEO_ID") or "").strip()
        creator_id = str(video_dict.get("CREATOR_ID") or "").strip()
        if not video_id or not creator_id:
            print("[db] upsert_video: VIDEO_ID hoặc CREATOR_ID rỗng, bỏ qua")
            return

        # BUG FIX: Phát hiện sớm khi CREATOR_ID bị nhầm thành numeric TikTok ID.
        # Nếu để lọt qua, MERGE sẽ tạo row mới vi phạm FK hoặc fail silently.
        if self._is_numeric_tiktok_id(creator_id):
            print(
                f"[db] upsert_video: CREATOR_ID '{creator_id}' là numeric TikTok ID "
                f"(sai schema — phải là username). Bỏ qua video {video_id}."
            )
            return

        # Kiểm tra CREATOR_ID tồn tại trong bảng CREATORS trước khi insert video.
        # Nếu không tồn tại, FK_VID_CR sẽ fail → rollback → comments cũng không lưu được.
        try:
            self.cursor.execute(
                "SELECT 1 FROM [CREATORS] WHERE [CREATOR_ID] = ?", creator_id
            )
            if not self.cursor.fetchone():
                print(
                    f"[db] upsert_video: CREATOR_ID '{creator_id}' chưa có trong bảng CREATORS. "
                    f"Tự động tạo row placeholder để tránh FK fail."
                )
                # Tạo row tối thiểu — upsert_creator() sẽ fill đầy đủ sau
                self.cursor.execute(
                    """
                    IF NOT EXISTS (SELECT 1 FROM [CREATORS] WHERE [CREATOR_ID] = ?)
                        INSERT INTO [CREATORS] ([CREATOR_ID], [SNAPSHOT_TIME])
                        VALUES (?, GETDATE())
                    """,
                    creator_id, creator_id,
                )
                self.conn.commit()
        except Exception as e:
            print(f"[db] upsert_video: Lỗi khi kiểm tra/tạo creator placeholder {creator_id}: {e}")

        row = {
            "VIDEO_ID":      video_id,
            "CREATOR_ID":    creator_id,
            "CREATE_TIME":   self._to_datetime(video_dict.get("CREATE_TIME")),
            "ANCHOR_TYPES":  self._anchor_types_to_str(video_dict.get("ANCHOR_TYPES")),
            "VIEW_COUNT":    self._to_int(video_dict.get("VIEW_COUNT")),
            "LIKE_COUNT":    self._to_int(video_dict.get("LIKE_COUNT")),
            "COMMENT_COUNT": self._to_int(video_dict.get("COMMENT_COUNT")),
            "SHARE_COUNT":   self._to_int(video_dict.get("SHARE_COUNT")),
            "SAVE_COUNT":    self._to_int(video_dict.get("SAVE_COUNT")),
            "VQSCORE":       self._to_float(video_dict.get("VQSCORE")),
            "BITRATE":       self._to_int(video_dict.get("BITRATE")),
            "CATEGORY_TYPE": self._to_int(video_dict.get("CATEGORY_TYPE")),
            "TITLE":         video_dict.get("TITLE"),
            "DESC":          video_dict.get("DESC"),
            "MUSIC_TITLE":   video_dict.get("MUSIC_TITLE"),
            "MUSIC_AUTHOR":  video_dict.get("MUSIC_AUTHOR"),
            "MUSIC_PLAY_URL": video_dict.get("MUSIC_PLAY_URL"),
            "SNAPSHOT_TIME": video_dict.get("SNAPSHOT_TIME") or datetime.now(),
            "RAW_JSON":      None,
        }
        row = {k: v for k, v in row.items() if v is not None}

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
        self._ensure_connection()
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
            return {str(r[0]) for r in self.cursor.fetchall() if r[0] is not None}
        except Exception as e:
            print(f"[db] Lỗi get_existing_comment_cids {video_id}: {e}")
            return set()

    def get_existing_reply_cids_for_comment(
        self,
        comment_id: str,
        video_id: str | None = None,
        creator_id: str | None = None,
    ) -> set[str]:
        self._ensure_connection()
        if not self.cursor:
            return set()
        try:
            if video_id is not None and creator_id is not None:
                self.cursor.execute(
                    """
                    SELECT [REPLY_ID] FROM [REPLIES]
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
            return {str(r[0]) for r in self.cursor.fetchall() if r[0] is not None}
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
        self._ensure_connection()
        if not self.cursor or not comments:
            return 0

        # BUG FIX: Kiểm tra (VIDEO_ID, CREATOR_ID) tồn tại trước khi insert bất kỳ comment nào.
        # Nếu video chưa có (ví dụ upsert_video() bị rollback do FK lỗi), toàn bộ batch
        # sẽ fail với FK_CMT_VIDEO violation → catch exception từng dòng không giúp được gì.
        # Kiểm tra trước giúp fail nhanh + log rõ ràng thay vì nuốt lỗi âm thầm.
        try:
            self.cursor.execute(
                "SELECT 1 FROM [VIDEOS] WHERE [VIDEO_ID] = ? AND [CREATOR_ID] = ?",
                str(video_id), str(creator_id),
            )
            if not self.cursor.fetchone():
                print(
                    f"[db] upsert_comments: VIDEO_ID='{video_id}' CREATOR_ID='{creator_id}' "
                    f"không tồn tại trong bảng VIDEOS. Bỏ qua {len(comments)} comments. "
                    f"(Nguyên nhân thường gặp: upsert_video() bị rollback do FK_VID_CR fail "
                    f"vì CREATOR_ID trong CREATORS không khớp — kiểm tra numeric vs username.)"
                )
                return 0
        except Exception as e:
            print(f"[db] upsert_comments: Lỗi khi kiểm tra video {video_id}: {e}")
            return 0

        skip_cids = skip_cids or set()
        available = self._table_columns("COMMENTS")
        saved = 0

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

                sort_extra = c.get("sort_extra_score") or {}
                user_obj = c.get("user") or {}

                row: dict = {
                    "COMMENT_ID": comment_id,
                    "VIDEO_ID": str(video_id),
                    "CREATOR_ID": str(creator_id),
                    "COMMENT_TIME": self._to_datetime(c.get("create_time")),
                    "LIKE_COUNT": self._to_int(c.get("digg_count")),
                    "REPLY_COUNT": self._to_int(c.get("reply_comment_total")),
                    "TEXT": c.get("text"),
                    "SNAPSHOT_TIME": datetime.now(),
                }

                optional: dict = {
                    "ROOT_COMMENT_ID": c.get("root_comment_id") or comment_id,
                    "COMMENT_LANGUAGE": c.get("comment_language"),
                    "IS_HIGH_PURCHASE_INTENT": self._to_bit(c.get("is_high_purchase_intent")),
                    "CUSTOM_VERIFY": c.get("custom_verify"),
                    "FOLD_STATUS": self._to_int(c.get("fold_status")),
                    "IS_AUTHOR_DIGGED": self._to_bit(c.get("is_author_digged")),
                    "LABEL_TEXTS": self._label_texts(c.get("label_list")),
                    "NO_SHOW": self._to_bit(c.get("no_show")),
                    "ENTERPRISE_VERIFY_REASON": c.get("enterprise_verify_reason"),
                    "REPLY_SCORE": self._to_float(sort_extra.get("reply_score")),
                    "SHOW_MORE_SCORE": self._to_float(sort_extra.get("show_more_score")),
                    "USER_UID": str(user_obj.get("uid") or "") or None,
                    "USER_UNIQUE_ID": user_obj.get("unique_id"),

                    # Theo yêu cầu: luôn để NULL
                    "RELATIVE_USERS": None,
                    "RAW_JSON": None,
                }

                for col, val in optional.items():
                    if col in available and val is not None:
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
        self._ensure_connection()
        if not self.cursor or not replies:
            return 0

        skip_cids = skip_cids or set()
        available = self._table_columns("REPLIES")
        saved = 0

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

                sort_extra = r.get("sort_extra_score") or {}
                user_obj = r.get("user") or {}

                row: dict = {
                    "REPLY_ID": reply_id,
                    "PARENT_CMT_ID": str(parent_comment_id),
                    "VIDEO_ID": str(video_id),
                    "CREATOR_ID": str(creator_id),
                    "REPLY_TIME": self._to_datetime(r.get("create_time")),
                    "LIKE_COUNT": self._to_int(r.get("digg_count")),
                    "REPLY_COUNT": self._to_int(r.get("reply_comment_total")),
                    "TEXT": r.get("text"),
                    "SNAPSHOT_TIME": datetime.now(),
                }

                optional: dict = {
                    "ROOT_COMMENT_ID": r.get("root_comment_id") or str(parent_comment_id),
                    "COMMENT_LANGUAGE": r.get("comment_language"),
                    "IS_HIGH_PURCHASE_INTENT": self._to_bit(r.get("is_high_purchase_intent")),
                    "CUSTOM_VERIFY": r.get("custom_verify"),
                    "FOLD_STATUS": self._to_int(r.get("fold_status")),
                    "IS_AUTHOR_DIGGED": self._to_bit(r.get("is_author_digged")),
                    "LABEL_TEXTS": self._label_texts(r.get("label_list")),
                    "NO_SHOW": self._to_bit(r.get("no_show")),
                    "ENTERPRISE_VERIFY_REASON": r.get("enterprise_verify_reason"),
                    "REPLY_SCORE": self._to_float(sort_extra.get("reply_score")),
                    "SHOW_MORE_SCORE": self._to_float(sort_extra.get("show_more_score")),
                    "USER_UID": str(user_obj.get("uid") or "") or None,
                    "USER_UNIQUE_ID": user_obj.get("unique_id"),

                    # Theo yêu cầu: luôn để NULL
                    "RELATIVE_USERS": None,
                    "RAW_JSON": None,
                }

                for col, val in optional.items():
                    if col in available and val is not None:
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