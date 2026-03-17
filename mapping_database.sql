/*
fix_creator_id_schema.sql
=========================
Script chuẩn hóa dữ liệu sau khi phát hiện bug: CREATOR_ID bị import
dưới dạng numeric TikTok ID thay vì username.

Triệu chứng:
  - Bảng VIDEOS có 2 loại CREATOR_ID: username ("banhsukem") và numeric ("6716730587635123")
  - Bảng COMMENTS rỗng hoàn toàn dù crawler chạy không lỗi
  - Log xuất hiện FK violation âm thầm bị nuốt bởi rollback

Nguyên nhân gốc:
  - Khi import Excel/CSV, cột CREATOR_ID bị lưu thành numeric TikTok ID
  - Crawler luôn dùng username làm CREATOR_ID khi lưu video/comment
  - FK VIDEOS → CREATORS fail do không khớp → toàn bộ comment không thể insert

Cách dùng:
  BƯỚC 1: Chạy phần 1 (DIAGNOSE) để xem tình trạng DB hiện tại.
  BƯỚC 2: Điền username vào bảng mapping (phần 2).
  BƯỚC 3: Chạy phần 3 (CLEAN) để xóa dữ liệu sai.
  BƯỚC 4: Chạy phần 4 (VERIFY) để kiểm tra lại.

Database: TikTok_Creator (MS SQL Server)
*/

USE [TikTok_Creator];
GO

-- ============================================================
-- PHẦN 1: DIAGNOSE — xem tình trạng hiện tại
-- ============================================================

-- 1a. Đếm tổng số CREATOR_ID theo loại
SELECT
    CASE
        WHEN [CREATOR_ID] NOT LIKE '%[^0-9]%' AND LEN([CREATOR_ID]) >= 15
            THEN 'numeric_id (SAI SCHEMA)'
        ELSE 'username (đúng)'
    END AS id_type,
    COUNT(*) AS count_rows
FROM [CREATORS]
GROUP BY
    CASE
        WHEN [CREATOR_ID] NOT LIKE '%[^0-9]%' AND LEN([CREATOR_ID]) >= 15
            THEN 'numeric_id (SAI SCHEMA)'
        ELSE 'username (đúng)'
    END;
GO

-- 1b. Liệt kê các CREATOR_ID dạng numeric (dữ liệu cần xử lý)
SELECT
    [CREATOR_ID],
    [CRAWL_STATUS],
    [FOLLOWERS],
    [SNAPSHOT_TIME]
FROM [CREATORS]
WHERE [CREATOR_ID] NOT LIKE '%[^0-9]%'
  AND LEN([CREATOR_ID]) >= 15
ORDER BY [SNAPSHOT_TIME] DESC;
GO

-- 1c. Kiểm tra VIDEOS bị gắn với numeric CREATOR_ID
SELECT
    v.[CREATOR_ID],
    COUNT(*) AS video_count
FROM [VIDEOS] v
WHERE v.[CREATOR_ID] NOT LIKE '%[^0-9]%'
  AND LEN(v.[CREATOR_ID]) >= 15
GROUP BY v.[CREATOR_ID]
ORDER BY video_count DESC;
GO

-- 1d. Kiểm tra VIDEOS bị gắn với username (đúng schema)
SELECT
    v.[CREATOR_ID],
    COUNT(*) AS video_count
FROM [VIDEOS] v
WHERE NOT (v.[CREATOR_ID] NOT LIKE '%[^0-9]%' AND LEN(v.[CREATOR_ID]) >= 15)
GROUP BY v.[CREATOR_ID]
ORDER BY video_count DESC;
GO


-- ============================================================
-- PHẦN 2: MAPPING TABLE — điền username tương ứng
-- ============================================================
-- Tạo bảng tạm để map numeric ID → username
-- Điền thủ công hoặc import từ file Excel gốc.

IF OBJECT_ID('tempdb..#id_mapping') IS NOT NULL DROP TABLE #id_mapping;

CREATE TABLE #id_mapping (
    numeric_id  NVARCHAR(50)  NOT NULL,   -- CREATOR_ID sai (numeric)
    username    NVARCHAR(50)  NOT NULL    -- CREATOR_ID đúng (username TikTok)
);

-- *** ĐIỀN VÀO ĐÂY — ví dụ: ***
-- INSERT INTO #id_mapping VALUES ('6716730587635123', 'banhsukem');
-- INSERT INTO #id_mapping VALUES ('7109367852692000', 'some_username');
-- (Lấy mapping từ file Excel gốc hoặc check thủ công trên TikTok)

-- Xem mapping đã điền
SELECT * FROM #id_mapping;
GO


-- ============================================================
-- PHẦN 3: CLEAN — xóa và chuẩn hóa
-- ============================================================
-- CẢNH BÁO: Phần này xóa dữ liệu. Chạy PHẦN 1 trước để biết rõ
-- tình trạng. Backup DB trước khi chạy nếu cần.

-- 3a. Xóa VIDEOS có CREATOR_ID là numeric (duplicate với videos sẽ được crawl lại)
-- Chỉ xóa video chưa có comment (video đã có comment thì cần cân nhắc kỹ hơn)
DELETE FROM [VIDEOS]
WHERE [CREATOR_ID] NOT LIKE '%[^0-9]%'
  AND LEN([CREATOR_ID]) >= 15
  AND NOT EXISTS (
      SELECT 1 FROM [COMMENTS] c
      WHERE c.[VIDEO_ID] = [VIDEOS].[VIDEO_ID]
        AND c.[CREATOR_ID] = [VIDEOS].[CREATOR_ID]
  );
GO

-- 3b. Xóa CREATOR_TAGS liên quan đến numeric CREATOR_ID
DELETE FROM [CREATOR_TAGS]
WHERE [CREATOR_ID] NOT LIKE '%[^0-9]%'
  AND LEN([CREATOR_ID]) >= 15;
GO

-- 3c. Xóa CREATORS có CREATOR_ID numeric
-- (Giữ lại row nếu có data quan trọng — check kỹ trước khi xóa)
DELETE FROM [CREATORS]
WHERE [CREATOR_ID] NOT LIKE '%[^0-9]%'
  AND LEN([CREATOR_ID]) >= 15;
GO

-- 3d. Reset CRAWL_STATUS của các username đã bị ảnh hưởng về 'pending'
-- để crawler chạy lại và lưu đúng schema
UPDATE [CREATORS]
SET [CRAWL_STATUS] = 'pending'
WHERE [CRAWL_STATUS] IN ('done', 'in_progress', 'error')
  AND NOT ([CREATOR_ID] NOT LIKE '%[^0-9]%' AND LEN([CREATOR_ID]) >= 15)
  AND NOT EXISTS (
      SELECT 1 FROM [VIDEOS] v
      WHERE v.[CREATOR_ID] = [CREATORS].[CREATOR_ID]
  );
-- Lưu ý: Chỉ reset creator nào CHƯA có video nào trong DB username-based
-- Creator đã có video đúng schema → giữ nguyên status
GO


-- ============================================================
-- PHẦN 4: VERIFY — kiểm tra lại sau khi clean
-- ============================================================

-- 4a. Xác nhận không còn numeric CREATOR_ID trong CREATORS
SELECT COUNT(*) AS remaining_numeric_creators
FROM [CREATORS]
WHERE [CREATOR_ID] NOT LIKE '%[^0-9]%'
  AND LEN([CREATOR_ID]) >= 15;
GO

-- 4b. Kiểm tra FK chain toàn vẹn
-- (kết quả phải = 0 nếu không có video nào trỏ đến creator không tồn tại)
SELECT COUNT(*) AS orphan_videos
FROM [VIDEOS] v
WHERE NOT EXISTS (
    SELECT 1 FROM [CREATORS] c WHERE c.[CREATOR_ID] = v.[CREATOR_ID]
);
GO

-- 4c. Kiểm tra orphan comments
SELECT COUNT(*) AS orphan_comments
FROM [COMMENTS] cm
WHERE NOT EXISTS (
    SELECT 1 FROM [VIDEOS] v
    WHERE v.[VIDEO_ID] = cm.[VIDEO_ID]
      AND v.[CREATOR_ID] = cm.[CREATOR_ID]
);
GO

-- 4d. Tóm tắt trạng thái sau clean
SELECT
    [CRAWL_STATUS],
    COUNT(*) AS creator_count
FROM [CREATORS]
GROUP BY [CRAWL_STATUS]
ORDER BY creator_count DESC;
GO

/*
============================================================
SAU KHI CHẠY SCRIPT:
============================================================
1. Chạy lại crawler — lần này CREATOR_ID sẽ nhất quán (username)
2. Kiểm tra log: không còn thấy "FK_VID_CR" hay "upsert_video rollback"
3. Bảng COMMENTS sẽ có dữ liệu sau khi crawl lại
============================================================
*/