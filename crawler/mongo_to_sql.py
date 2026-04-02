"""
migrate_mongo_to_sql.py
=======================
Script chạy 1 lần: đọc toàn bộ creator từ MongoDB → ghi vào bảng CREATORS (SQL Server).

Lưu ý: script idempotent — nếu chạy lại lần hai nó sẽ UPDATE thay vì INSERT, và giữ nguyên CRAWL_STATUS đang có 
(vì CRAWL_STATUS trong row chỉ set là 'pending' khi MERGE — nếu row đã tồn tại thì cột này bị UPDATE về 'pending' lại). 
Nếu bạn không muốn reset status của creator đã done khi chạy lại migrate, cho tôi biết để tôi thêm điều kiện exclude.

Mapping field:
  MongoDB          →  SQL CREATORS
  ─────────────────────────────────────────
  ID               →  CREATOR_ID          (username TikTok — dùng để crawl)
  Followers        →  FOLLOWERS
  Engagement       →  ENGAGEMENT
  Median Views     →  MEDIAN_VIEWS
  Start Price      →  PRICE
  Collab Score     →  COLLAB_SCORE
  Tags             →  CREATOR_TAGS (bảng riêng)
  CRAWL_STATUS     →  'pending'           (mặc định, chưa crawl)

Các field MongoDB không có cột tương ứng trong schema hiện tại
(Name, Country, Broadcast Score) sẽ được bỏ qua.

Chạy:
  python migrate_mongo_to_sql.py

Idempotent: chạy lại nhiều lần không bị trùng (dùng MERGE).
"""

from __future__ import annotations

import os
import re
import json
from datetime import datetime

import pymongo
import pyodbc


# ===========================================================================
# CONFIG — chỉnh tại đây nếu cần
# ===========================================================================

MONGO_URI           = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB            = "tiktok_ads_db"
MONGO_COLLECTION    = "sample_direction1_category_stratified_part1"

#sample_direction1_random, sample_direction1_category_stratified, sample_direction2_market_reflective

SQL_DRIVER   = os.getenv("SQL_DRIVER",   "ODBC Driver 17 for SQL Server")
SQL_SERVER   = os.getenv("SQL_SERVER",   r"ACER-TANPHAT\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "TikTok_Creator_DB_v1")

BATCH_SIZE = 3000  # số creator commit mỗi lần (tránh transaction quá lớn)


# ===========================================================================
# HELPERS
# ===========================================================================

def build_conn_str() -> str:
    trusted = os.getenv("SQL_SERVER_TRUSTED_CONNECTION", "1").strip().lower() in {"1","true","yes"}
    parts = [
        f"DRIVER={{{SQL_DRIVER}}}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}",
    ]
    if trusted:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={os.getenv('SQL_USERNAME','')}")
        parts.append(f"PWD={os.getenv('SQL_PASSWORD','')}")
    return ";".join(parts) + ";"


def to_int(v):
    if v is None:
        return None
    try:
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def to_float(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def normalize_tags(raw) -> list[str]:
    """Chuyển Tags từ MongoDB (string hoặc list) thành list string sạch."""
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    else:
        items = re.split(r"[|,;/]+", str(raw))
    seen, out = set(), []
    for item in items:
        tag = str(item).strip()
        if tag and tag.lower() not in seen:
            seen.add(tag.lower())
            out.append(tag)
    return out


def upsert_creator(cursor, row: dict):
    """MERGE 1 creator vào bảng CREATORS."""
    cols   = [c for c, v in row.items() if v is not None]
    non_keys = [c for c in cols if c != "CREATOR_ID"]

    src_cols    = ", ".join([f"? AS [{c}]" for c in cols])
    insert_cols = ", ".join([f"[{c}]" for c in cols])
    insert_vals = ", ".join([f"src.[{c}]" for c in cols])

    sql = f"""
    MERGE [CREATORS] AS target
    USING (SELECT {src_cols}) AS src
    ON target.[CREATOR_ID] = src.[CREATOR_ID]
    """
    if non_keys:
        set_clause = ", ".join([f"target.[{c}] = src.[{c}]" for c in non_keys])
        sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
    sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"

    cursor.execute(sql, [row[c] for c in cols])


def sync_tags(cursor, creator_id: str, tags: list[str]):
    """Upsert tags và gắn vào CREATOR_TAGS."""
    if not tags:
        return

    tag_ids = []
    for tag_name in tags:
        cursor.execute(
            """
            MERGE [TAGS] AS target
            USING (SELECT ? AS [TAG_NAME]) AS src
            ON target.[TAG_NAME] = src.[TAG_NAME]
            WHEN NOT MATCHED THEN INSERT ([TAG_NAME]) VALUES (src.[TAG_NAME]);
            """,
            tag_name,
        )
        cursor.execute("SELECT [TAG_ID] FROM [TAGS] WHERE [TAG_NAME] = ?", tag_name)
        r = cursor.fetchone()
        if r:
            tag_ids.append(int(r[0]))

    # Xóa tag cũ rồi insert lại (idempotent)
    cursor.execute("DELETE FROM [CREATOR_TAGS] WHERE [CREATOR_ID] = ?", creator_id)
    for tag_id in tag_ids:
        cursor.execute(
            "INSERT INTO [CREATOR_TAGS] ([CREATOR_ID], [TAG_ID]) VALUES (?, ?)",
            creator_id, tag_id,
        )


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    # ---- Kết nối MongoDB ----
    print(f"[migrate] Kết nối MongoDB: {MONGO_URI} / {MONGO_DB}.{MONGO_COLLECTION}")
    mongo_client = pymongo.MongoClient(MONGO_URI)
    collection   = mongo_client[MONGO_DB][MONGO_COLLECTION]

    total_mongo = collection.count_documents({})
    print(f"[migrate] Tổng documents trong MongoDB: {total_mongo}")


    # ---- Kết nối SQL Server ----
    print(f"[migrate] Kết nối SQL Server: {SQL_SERVER} / {SQL_DATABASE}")
    conn   = pyodbc.connect(build_conn_str(), timeout=10)
    cursor = conn.cursor()
    print("[migrate] Kết nối SQL Server thành công")

    # ---- Migrate ----
    fields = {
        "_id": 1, "creator_id": 1, "followers_num": 1, "engagement_num": 1,
        "median_views_num": 1, "price_num": 1, "collab_score_num": 1, "category": 1, "broadcast_score_num": 1,
    }

    processed = 0
    skipped   = 0
    batch_n   = 0

    for doc in collection.find({}, fields):
        creator_id = str(doc.get("creator_id") or "").strip()
        if not creator_id:
            skipped += 1
            continue

        price = to_float(doc.get("Start Price"))

        row = {
            "CREATOR_ID":        creator_id,
            "FOLLOWERS":         to_int(doc.get("followers_num")),
            "ENGAGEMENT":        to_float(doc.get("engagement_num")),
            "MEDIAN_VIEWS":      to_int(doc.get("median_views_num")),
            "PRICE":             price,
            "MISSING_PRICE_FLAG": 0 if price is not None else 1,
            "COLLAB_SCORE":      to_float(doc.get("collab_score_num")),
            "CRAWL_STATUS":      "pending",
            "SNAPSHOT_TIME":     datetime.now(),
            "BROADCAST_SCORE": to_float(doc.get("broadcast_score_num")),
        }
        # Bỏ None để MERGE không overwrite giá trị cũ bằng NULL
        row = {k: v for k, v in row.items() if v is not None}
        # MISSING_PRICE_FLAG và CRAWL_STATUS luôn cần có
        row.setdefault("MISSING_PRICE_FLAG", 1)
        row.setdefault("CRAWL_STATUS", "pending")

        tags = normalize_tags(doc.get("category"))

        try:
            upsert_creator(cursor, row)
            sync_tags(cursor, creator_id, tags)
            processed += 1
            batch_n   += 1
        except Exception as e:
            print(f"[migrate] Lỗi creator {creator_id}: {e}")
            conn.rollback()
            skipped += 1
            batch_n  = 0
            continue

        # Commit theo batch
        if batch_n >= BATCH_SIZE:
            conn.commit()
            print(f"[migrate] Đã commit {processed}/{total_mongo} ...")
            batch_n = 0

    # Commit phần còn lại
    if batch_n > 0:
        conn.commit()

    print(f"\n[migrate] Hoàn thành!")
    print(f"  Đã import : {processed} creators")
    print(f"  Bỏ qua    : {skipped} (ID rỗng hoặc lỗi)")

    cursor.close()
    conn.close()
    mongo_client.close()


if __name__ == "__main__":
    main()