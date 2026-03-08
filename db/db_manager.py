"""
db/db_manager.py
================
Quản lý kết nối và thao tác với SQL Server.
Lưu trữ: creator profile stats, video stats.

Tables (SQL Server):
  - Creators : creator_id (PK), followers_count, total_likes, crawled_at
  - Videos   : video_id (PK), creator_id (FK), create_time, view/like/comment/save counts, crawled_at
"""

import os
from datetime import datetime

import pyodbc


# ===========================================================================
# CẤU HÌNH — đọc từ env hoặc dùng default
# ===========================================================================
SQL_SERVER   = os.getenv("SQL_SERVER",   r"ACER-TANPHAT\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "TikTok_Creator_crawl")
SQL_DRIVER   = os.getenv("SQL_DRIVER",   "ODBC Driver 17 for SQL Server")

CONN_STR = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    "Trusted_Connection=yes;"
)


class DBManager:
    """
    SQL Server manager cho creator/video stats.
    Dùng song song với MongoCommentDB — không thay thế.
    """

    def __init__(self):
        self.conn   = None
        self.cursor = None
        self._connect()

    # ------------------------------------------------------------------
    # KẾT NỐI
    # ------------------------------------------------------------------

    def _connect(self):
        try:
            self.conn   = pyodbc.connect(CONN_STR, timeout=10)
            self.cursor = self.conn.cursor()
            self._ensure_tables()
            print(f"[db] Kết nối SQL Server thành công: {SQL_DATABASE}")
        except Exception as e:
            print(f"[db] Lỗi kết nối SQL Server: {e}")
            self.conn   = None
            self.cursor = None

    def _ensure_tables(self):
        """Tạo bảng nếu chưa có."""
        self.cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='Creators' AND xtype='U'
            )
            CREATE TABLE Creators (
                creator_id       NVARCHAR(200) PRIMARY KEY,
                followers_count  BIGINT,
                total_likes      BIGINT,
                followers_raw    NVARCHAR(50),
                likes_raw        NVARCHAR(50),
                crawled_at       DATETIME DEFAULT GETDATE()
            )
        """)
        self.cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='Videos' AND xtype='U'
            )
            CREATE TABLE Videos (
                video_id         NVARCHAR(50)  PRIMARY KEY,
                creator_id       NVARCHAR(200),
                video_url        NVARCHAR(500),
                create_time      DATETIME,
                view_count       BIGINT,
                like_count       BIGINT,
                comment_count    BIGINT,
                save_count       BIGINT,
                crawled_at       DATETIME DEFAULT GETDATE()
            )
        """)
        self.conn.commit()

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
    # CREATORS
    # ------------------------------------------------------------------

    def creator_exists(self, creator_id: str) -> bool:
        """Kiểm tra creator đã có trong DB chưa."""
        if not self.cursor:
            return False
        try:
            self.cursor.execute(
                "SELECT 1 FROM Creators WHERE creator_id = ?", creator_id
            )
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"[db] Lỗi creator_exists {creator_id}: {e}")
            return False

    def upsert_creator(self, creator_id: str, stats: dict):
        """Upsert thông tin creator vào bảng Creators."""
        if not self.cursor:
            return
        try:
            self.cursor.execute("""
                MERGE Creators AS target
                USING (SELECT ? AS creator_id) AS src ON target.creator_id = src.creator_id
                WHEN MATCHED THEN UPDATE SET
                    followers_count = ?,
                    total_likes     = ?,
                    followers_raw   = ?,
                    likes_raw       = ?,
                    crawled_at      = GETDATE()
                WHEN NOT MATCHED THEN INSERT
                    (creator_id, followers_count, total_likes, followers_raw, likes_raw)
                VALUES (?, ?, ?, ?, ?);
            """,
                creator_id,
                stats.get("followers_count"),
                stats.get("total_likes"),
                stats.get("followers_count_raw"),
                stats.get("total_likes_raw"),
                creator_id,
                stats.get("followers_count"),
                stats.get("total_likes"),
                stats.get("followers_count_raw"),
                stats.get("total_likes_raw"),
            )
            self.conn.commit()
            print(f"[db] Upsert creator: {creator_id}")
        except Exception as e:
            print(f"[db] Lỗi upsert creator {creator_id}: {e}")
            self.conn.rollback()

    # ------------------------------------------------------------------
    # VIDEOS
    # ------------------------------------------------------------------

    def video_exists(self, video_id: str, creator_id: str) -> bool:
        """Kiểm tra video đã có trong DB chưa."""
        if not self.cursor:
            return False
        try:
            self.cursor.execute(
                "SELECT 1 FROM Videos WHERE video_id = ? AND creator_id = ?",
                video_id, creator_id,
            )
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"[db] Lỗi video_exists {video_id}: {e}")
            return False

    def upsert_video(self, creator_id: str, stats: dict):
        """Upsert thông tin video vào bảng Videos."""
        if not self.cursor:
            return
        video_id = stats.get("video_id")
        if not video_id:
            return
        try:
            self.cursor.execute("""
                MERGE Videos AS target
                USING (SELECT ? AS video_id) AS src ON target.video_id = src.video_id
                WHEN MATCHED THEN UPDATE SET
                    view_count    = ?,
                    like_count    = ?,
                    comment_count = ?,
                    save_count    = ?,
                    crawled_at    = GETDATE()
                WHEN NOT MATCHED THEN INSERT
                    (video_id, creator_id, video_url, create_time,
                     view_count, like_count, comment_count, save_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
                video_id,
                stats.get("view_count"),
                stats.get("like_count"),
                stats.get("comment_count"),
                stats.get("save_count_ui"),
                video_id,
                creator_id,
                stats.get("video_url"),
                stats.get("create_time"),
                stats.get("view_count"),
                stats.get("like_count"),
                stats.get("comment_count"),
                stats.get("save_count_ui"),
            )
            self.conn.commit()
            print(f"[db] Upsert video: {video_id}")
        except Exception as e:
            print(f"[db] Lỗi upsert video {video_id}: {e}")
            self.conn.rollback()
