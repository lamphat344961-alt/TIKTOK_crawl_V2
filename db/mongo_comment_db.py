"""
mongo_comment_db.py
===================
Quản lý lưu trữ raw comment/reply vào MongoDB.

Collections:
  - tiktok_comments_raw : mỗi document = 1 comment (raw JSON từ TikTok API)
  - tiktok_replies_raw  : mỗi document = 1 reply   (raw JSON từ TikTok API)

Index:
  - tiktok_comments_raw : unique (cid, aweme_id)
  - tiktok_replies_raw  : unique (cid, aweme_id)  ← giống comment, cid reply là duy nhất trong video

Lý do dùng (cid, aweme_id) cho cả reply:
  - cid = ID của reply, do TikTok sinh ra, unique trong 1 video
  - reply_id (parent comment id) không cần vào index vì không quyết định uniqueness
  - tránh conflict khi TikTok thay đổi thread structure (reply→reply)
"""

import os
from datetime import datetime

import pymongo
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError


# ===========================================================================
# CẤU HÌNH
# ===========================================================================
MONGO_URI    = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME      = "tiktok_creators_db"
COL_COMMENTS = "tiktok_comments_raw"
COL_REPLIES  = "tiktok_replies_raw"


class MongoCommentDB:
    """
    Quản lý lưu raw comment và reply vào MongoDB.
    Dùng song song với DBManager (SQL Server) — không thay thế.
    """

    def __init__(self):
        self.client        = None
        self.db            = None
        self._col_comments = None
        self._col_replies  = None
        self._connect()

    # ------------------------------------------------------------------
    # KẾT NỐI & KHỞI TẠO
    # ------------------------------------------------------------------

    def _connect(self):
        try:
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.db            = self.client[DB_NAME]
            self._col_comments = self.db[COL_COMMENTS]
            self._col_replies  = self.db[COL_REPLIES]
            self._ensure_indexes()
            print(f"[mongo_comment] Kết nối MongoDB thành công: {DB_NAME}")
        except Exception as e:
            print(f"[mongo_comment] Lỗi kết nối MongoDB: {e}")
            self.client = None

    def _ensure_indexes(self):
        """Tạo unique index để tránh duplicate."""
        try:
            # --- Comments: unique (cid, aweme_id) ---
            self._col_comments.create_index(
                [("cid", pymongo.ASCENDING), ("aweme_id", pymongo.ASCENDING)],
                unique=True,
                name="idx_comment_cid_awemeid",
            )

            # --- Replies: unique (cid, aweme_id) ---
            # cid của reply là unique trong phạm vi 1 video
            # KHÔNG dùng (cid, reply_id) vì reply_id có thể thay đổi theo thread structure
            self._col_replies.create_index(
                [("cid", pymongo.ASCENDING), ("aweme_id", pymongo.ASCENDING)],
                unique=True,
                name="idx_reply_cid_awemeid",
            )

            # Index phụ: query theo video
            self._col_comments.create_index(
                [("aweme_id", pymongo.ASCENDING)],
                name="idx_comment_awemeid",
            )
            self._col_replies.create_index(
                [("aweme_id", pymongo.ASCENDING)],
                name="idx_reply_awemeid",
            )

            # Index phụ: query replies theo parent comment (dùng khi check duplicate per-comment)
            self._col_replies.create_index(
                [("reply_id", pymongo.ASCENDING)],
                name="idx_reply_replyid",
            )
        except Exception as e:
            print(f"[mongo_comment] Lỗi tạo index: {e}")

    def close(self):
        if self.client:
            self.client.close()
            print("[mongo_comment] Đã đóng kết nối MongoDB")

    # ------------------------------------------------------------------
    # DUPLICATE DETECTION
    # ------------------------------------------------------------------

    def get_existing_comment_cids(self, video_id: str) -> set:
        """
        Lấy set cid comments đã có trong DB cho 1 video.
        Comments thường ít (vài nghìn max) → safe để load per-video.
        """
        if self._col_comments is None:
            return set()
        try:
            docs = self._col_comments.find(
                {"aweme_id": str(video_id)},
                {"cid": 1, "_id": 0},
            )
            return {doc["cid"] for doc in docs}
        except Exception as e:
            print(f"[mongo_comment] Lỗi get comment cids {video_id}: {e}")
            return set()

    def get_existing_reply_cids_for_comment(self, comment_id: str) -> set:
        """
        Lấy set cid replies đã có cho 1 comment cụ thể.

        Scope PER-COMMENT (không per-video) vì:
          - Video lớn có thể có 200k+ replies tổng cộng
          - Load toàn bộ per-video lên memory rất nặng
          - Reply được fetch tuần tự từng comment → per-comment là đủ

        Filter theo reply_id = comment_id
        (reply_id trong TikTok API = parent comment id).
        """
        if self._col_replies is None:
            return set()
        try:
            docs = self._col_replies.find(
                {"reply_id": str(comment_id)},
                {"cid": 1, "_id": 0},
            )
            return {doc["cid"] for doc in docs}
        except Exception as e:
            print(f"[mongo_comment] Lỗi get reply cids comment {comment_id}: {e}")
            return set()

    # ------------------------------------------------------------------
    # UPSERT COMMENTS (bulk)
    # ------------------------------------------------------------------

    def upsert_comments(
        self,
        comments: list[dict],
        creator_id: str,
        video_id: str,
        skip_cids: set | None = None,
    ) -> int:
        """
        Upsert danh sách raw comments vào tiktok_comments_raw.

        Mỗi document = raw TikTok API object + meta fields:
          _crawled_at : datetime lúc crawl
          _creator_id : username creator
          _video_id   : video ID (redundant với aweme_id, để tiện query)

        Note về reply_comment[] preview:
          TikTok comment object thường kèm reply_comment[] preview (1-2 replies).
          Preview này được lưu NGUYÊN trong comment document.
          Replies đầy đủ sẽ được fetch riêng và lưu vào tiktok_replies_raw.
          Không bị duplicate vì tiktok_replies_raw có unique index riêng.

        Returns:
            Số documents được ghi thành công (inserted + modified)
        """
        if self._col_comments is None or not comments:
            return 0

        skip_cids = skip_cids or set()
        now       = datetime.now()
        ops       = []

        for c in comments:
            cid = c.get("cid")
            if not cid or cid in skip_cids:
                continue

            doc = dict(c)
            doc["_crawled_at"] = now
            doc["_creator_id"] = creator_id
            doc["_video_id"]   = str(video_id)
            doc["aweme_id"]    = str(doc.get("aweme_id", video_id))

            ops.append(UpdateOne(
                {"cid": cid, "aweme_id": str(video_id)},
                {"$set": doc},
                upsert=True,
            ))

        if not ops:
            return 0

        try:
            result = self._col_comments.bulk_write(ops, ordered=False)
            total  = result.upserted_count + result.modified_count
            print(f"[mongo_comment] Comments: {result.upserted_count} mới, "
                  f"{result.modified_count} cập nhật (video {video_id})")
            return total
        except BulkWriteError as bwe:
            written = bwe.details.get("nUpserted", 0) + bwe.details.get("nModified", 0)
            print(f"[mongo_comment] BulkWriteError comments: {written} ghi được "
                  f"({len(bwe.details.get('writeErrors', []))} lỗi trùng key)")
            return written
        except Exception as e:
            print(f"[mongo_comment] Lỗi upsert comments video {video_id}: {e}")
            return 0

    # ------------------------------------------------------------------
    # UPSERT REPLIES (bulk, per-comment)
    # ------------------------------------------------------------------

    def upsert_replies(
        self,
        replies: list[dict],
        creator_id: str,
        video_id: str,
        parent_comment_id: str,
        skip_cids: set | None = None,
    ) -> int:
        """
        Upsert danh sách raw replies vào tiktok_replies_raw.

        Lưu ý về TikTok reply structure:
          cid               = ID của reply này
          reply_id          = ID của comment đang được reply (= parent comment)
          reply_to_reply_id = ID reply đang được reply ("0" nếu reply thẳng vào comment)

        skip_cids = set cid đã có cho comment này cụ thể
        (lấy từ get_existing_reply_cids_for_comment — scope per-comment).

        Returns:
            Số documents được ghi thành công
        """
        if self._col_replies is None or not replies:
            return 0

        skip_cids = skip_cids or set()
        now       = datetime.now()
        ops       = []

        for r in replies:
            cid = r.get("cid")
            if not cid or cid in skip_cids:
                continue

            doc = dict(r)
            doc["_crawled_at"]        = now
            doc["_creator_id"]        = creator_id
            doc["_video_id"]          = str(video_id)
            doc["_parent_comment_id"] = str(parent_comment_id)
            doc["aweme_id"]           = str(doc.get("aweme_id", video_id))

            ops.append(UpdateOne(
                {"cid": cid, "aweme_id": str(video_id)},
                {"$set": doc},
                upsert=True,
            ))

        if not ops:
            return 0

        try:
            result = self._col_replies.bulk_write(ops, ordered=False)
            total  = result.upserted_count + result.modified_count
            print(f"[mongo_comment] Replies: {result.upserted_count} mới, "
                  f"{result.modified_count} cập nhật (comment {parent_comment_id})")
            return total
        except BulkWriteError as bwe:
            written = bwe.details.get("nUpserted", 0) + bwe.details.get("nModified", 0)
            print(f"[mongo_comment] BulkWriteError replies: {written} ghi được")
            return written
        except Exception as e:
            print(f"[mongo_comment] Lỗi upsert replies comment {parent_comment_id}: {e}")
            return 0
