/*
Created        15/03/2026
Project        TikTok Creator Analysis - Updated schema from Col_mean.xlsx
Database       MS SQL Server

Muc tieu:
- Xoa database cu neu da ton tai
- Tao lai schema tu dau
- Giu cau truc du an cu
- Bo sung cac field moi tu file Col_mean.xlsx vao cac bang CREATORS, VIDEOS, COMMENTS, REPLIES
- Luu y: CREATOR_ID trong schema moi duoc hieu la ma dinh danh creator tren TikTok
*/

USE master;
GO

IF DB_ID(N'TikTok_Creator') IS NOT NULL
BEGIN
    ALTER DATABASE [TikTok_Creator] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [TikTok_Creator];
END
GO

CREATE DATABASE [TikTok_Creator];
GO

USE [TikTok_Creator];
GO

-- ============================================================
-- 1. TAGS
-- ============================================================
CREATE TABLE [dbo].[TAGS] (
    [TAG_ID]    INT             NOT NULL IDENTITY(1,1),
    [TAG_NAME]  NVARCHAR(100)   NOT NULL,

    CONSTRAINT [PK_TAGS]     PRIMARY KEY ([TAG_ID]),
    CONSTRAINT [UQ_TAG_NAME] UNIQUE ([TAG_NAME])
);
GO

-- ============================================================
-- 2. CREATORS
-- ============================================================
CREATE TABLE [dbo].[CREATORS] (
    [CREATOR_ID]            NVARCHAR(50)   NOT NULL,   -- Ma dinh danh duy nhat cua creator tren TikTok
    [FOLLOWERS]             BIGINT          NULL,       -- Tong follower
    [FOLLOWING_COUNT]       BIGINT          NULL,       -- So tai khoan creator dang theo doi
    [FRIEND_COUNT]          INT             NULL,       -- So luong ban be / mutual
    [ENGAGEMENT]            DECIMAL(10,4)   NULL,       -- Chi so tuong tac tong quat
    [MEDIAN_VIEWS]          BIGINT          NULL,       -- Trung vi view video
    [TOTAL_LIKES]           BIGINT          NULL,       -- Tong luot thich toan kenh
    [DIGG_COUNT]            BIGINT          NULL,       -- Tong so luot creator da tha tim cho noi dung khac
    [VIDEO_COUNT]           BIGINT          NULL,       -- Tong so video tren kenh
    [COLLAB_SCORE]          DECIMAL(10,4)   NULL,       -- Diem hop tac thuong mai
    [PRICE]                 DECIMAL(18,2)   NULL,       -- Gia hop tac quang cao
    [MISSING_PRICE_FLAG]    BIT             NOT NULL CONSTRAINT [DF_CREATORS_MISSING_PRICE_FLAG] DEFAULT (1),
    [CATEGORY]              NVARCHAR(100)   NULL,       -- Danh muc creator
    [SNAPSHOT_TIME]         DATETIME2       NOT NULL,   -- Thoi diem crawl
    [RAW_JSON]              NVARCHAR(MAX)   NULL,       -- JSON goc response creator/profile

    CONSTRAINT [PK_CREATORS] PRIMARY KEY ([CREATOR_ID])
);
GO

-- ============================================================
-- 3. CREATOR_TAGS
-- ============================================================
CREATE TABLE [dbo].[CREATOR_TAGS] (
    [CREATOR_ID]    NVARCHAR(50)   NOT NULL,
    [TAG_ID]        INT             NOT NULL,

    CONSTRAINT [PK_CREATOR_TAGS] PRIMARY KEY ([CREATOR_ID], [TAG_ID]),
    CONSTRAINT [FK_CT_CREATOR] FOREIGN KEY ([CREATOR_ID])
        REFERENCES [dbo].[CREATORS]([CREATOR_ID])
        ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT [FK_CT_TAG] FOREIGN KEY ([TAG_ID])
        REFERENCES [dbo].[TAGS]([TAG_ID])
        ON UPDATE NO ACTION ON DELETE NO ACTION
);
GO

-- ============================================================
-- 4. VIDEOS
-- ============================================================
CREATE TABLE [dbo].[VIDEOS] (
    [VIDEO_ID]          NVARCHAR(50)   NOT NULL,
    [CREATOR_ID]        NVARCHAR(50)   NOT NULL,
    [CREATE_TIME]       DATETIME2       NULL,       -- Thoi diem dang video
	[ANCHOR_TYPES]		NVARCHAR(MAX) NULL,				
    [VIEW_COUNT]        BIGINT          NULL,       -- Luot xem
    [LIKE_COUNT]        BIGINT          NULL,       -- Luot thich
    [COMMENT_COUNT]     BIGINT          NULL,       -- So comment
    [SHARE_COUNT]       BIGINT          NULL,       -- Luot chia se
    [SAVE_COUNT]        BIGINT          NULL,       -- Luot luu
    [VQSCORE]           DECIMAL(10,2)   NULL,       -- Diem chat luong video
    [BITRATE]           BIGINT          NULL,       -- Bitrate video
    [CATEGORY_TYPE]     INT             NULL,       -- Ma loai noi dung video
    [TITLE]             NVARCHAR(500)   NULL,       -- Tieu de video
    [DESC]              NVARCHAR(MAX)   NULL,       -- Mo ta / caption / hashtag
    [MUSIC_TITLE]       NVARCHAR(100)   NULL,       -- Ten bai nhac/album
    [MUSIC_AUTHOR]      NVARCHAR(100)   NULL,       -- Tac gia / nghe si nhac nen
    [SNAPSHOT_TIME]     DATETIME2       NOT NULL,   -- Thoi diem crawl
    [RAW_JSON]          NVARCHAR(MAX)   NULL,       -- JSON goc response video

    CONSTRAINT [PK_VIDEOS] PRIMARY KEY ([VIDEO_ID], [CREATOR_ID]),
    CONSTRAINT [FK_VID_CR] FOREIGN KEY ([CREATOR_ID])
        REFERENCES [dbo].[CREATORS]([CREATOR_ID])
        ON UPDATE NO ACTION ON DELETE NO ACTION
);
GO

-- ============================================================
-- 5. COMMENTS (comment goc - top level)
-- ============================================================
CREATE TABLE [dbo].[COMMENTS] (
    [COMMENT_ID]                NVARCHAR(50)   NOT NULL,
    [VIDEO_ID]                  NVARCHAR(50)   NOT NULL,
    [CREATOR_ID]                NVARCHAR(50)   NOT NULL,
    [ROOT_COMMENT_ID]           NVARCHAR(50)   NULL,       -- Comment goc cua thread; voi top-level thuong bang COMMENT_ID
    [COMMENT_TIME]              DATETIME2       NULL,
    [LIKE_COUNT]                BIGINT          NULL,
    [REPLY_COUNT]               INT             NULL,
    [TEXT]                      NVARCHAR(MAX)   NULL,
    [COMMENT_LANGUAGE]          VARCHAR(20)     NULL,
    [IS_HIGH_PURCHASE_INTENT]   BIT             NULL,
    [CUSTOM_VERIFY]             NVARCHAR(50)   NULL,
    [FOLD_STATUS]               INT             NULL,
    [IS_AUTHOR_DIGGED]          BIT             NULL,
    [LABEL_TEXTS]               NVARCHAR(MAX)   NULL,
    [NO_SHOW]                   BIT             NULL,
    [ENTERPRISE_VERIFY_REASON]  NVARCHAR(MAX)   NULL,
    [RELATIVE_USERS]            NVARCHAR(MAX)   NULL,       -- JSON / raw text
    [REPLY_SCORE]               DECIMAL(18,6)   NULL,
    [SHOW_MORE_SCORE]           DECIMAL(18,6)   NULL,
    [RAW_JSON]                  NVARCHAR(MAX)   NULL,
	[USER_UID]					NVARCHAR(50)   NULL,
	[USER_UNIQUE_ID]			NVARCHAR(100)  NULL,
    [SNAPSHOT_TIME]             DATETIME2       NOT NULL,

    CONSTRAINT [PK_COMMENTS] PRIMARY KEY ([COMMENT_ID], [VIDEO_ID], [CREATOR_ID]),
    CONSTRAINT [FK_CMT_VIDEO] FOREIGN KEY ([VIDEO_ID], [CREATOR_ID])
        REFERENCES [dbo].[VIDEOS]([VIDEO_ID], [CREATOR_ID])
        ON UPDATE NO ACTION ON DELETE NO ACTION
);
GO

-- ============================================================
-- 6. REPLIES (reply cua comment goc / nested reply)
-- ============================================================
CREATE TABLE [dbo].[REPLIES] (
    [REPLY_ID]                  NVARCHAR(50)   NOT NULL,
    [PARENT_CMT_ID]             NVARCHAR(50)   NOT NULL,   -- Comment cha truc tiep
    [VIDEO_ID]                  NVARCHAR(50)   NOT NULL,
    [CREATOR_ID]                NVARCHAR(50)   NOT NULL,
    [ROOT_COMMENT_ID]           NVARCHAR(50)   NULL,       -- Comment goc cua thread
    [REPLY_TIME]                DATETIME2       NULL,
    [LIKE_COUNT]                BIGINT          NULL,
    [REPLY_COUNT]               INT             NULL,       -- So reply truc tiep cua reply neu API co
    [TEXT]                      NVARCHAR(MAX)   NULL,
    [COMMENT_LANGUAGE]          VARCHAR(20)     NULL,
    [IS_HIGH_PURCHASE_INTENT]   BIT             NULL,
    [CUSTOM_VERIFY]             NVARCHAR(100)   NULL,
    [FOLD_STATUS]               INT             NULL,
    [IS_AUTHOR_DIGGED]          BIT             NULL,
    [LABEL_TEXTS]               NVARCHAR(MAX)   NULL,
    [NO_SHOW]                   BIT             NULL,
    [ENTERPRISE_VERIFY_REASON]  NVARCHAR(MAX)   NULL,
    [RELATIVE_USERS]            NVARCHAR(MAX)   NULL,
    [REPLY_SCORE]               DECIMAL(18,6)   NULL,
    [SHOW_MORE_SCORE]           DECIMAL(18,6)   NULL,
    [RAW_JSON]                  NVARCHAR(MAX)   NULL,
	[USER_UID]					NVARCHAR(50)   NULL,
	[USER_UNIQUE_ID]			NVARCHAR(50)  NULL,
    [SNAPSHOT_TIME]             DATETIME2       NOT NULL,

    CONSTRAINT [PK_REPLIES] PRIMARY KEY ([REPLY_ID], [PARENT_CMT_ID], [VIDEO_ID], [CREATOR_ID]),
    CONSTRAINT [FK_RP_COMMENT] FOREIGN KEY ([PARENT_CMT_ID], [VIDEO_ID], [CREATOR_ID])
        REFERENCES [dbo].[COMMENTS]([COMMENT_ID], [VIDEO_ID], [CREATOR_ID])
        ON UPDATE NO ACTION ON DELETE NO ACTION
);
GO

-- ============================================================
-- 7. INDEXES
-- ============================================================

-- VIDEOS
CREATE INDEX [IDX_VID_CREATOR_TIME] ON [dbo].[VIDEOS] ([CREATOR_ID], [CREATE_TIME]);
GO

CREATE INDEX [IDX_VID_CREATE_TIME] ON [dbo].[VIDEOS] ([CREATE_TIME]);
GO

CREATE INDEX [IDX_VID_CATEGORY_TYPE] ON [dbo].[VIDEOS] ([CATEGORY_TYPE]);
GO

-- COMMENTS
CREATE INDEX [IDX_CMT_VIDEO] ON [dbo].[COMMENTS] ([VIDEO_ID]);
GO

CREATE INDEX [IDX_CMT_CREATOR_TIME] ON [dbo].[COMMENTS] ([CREATOR_ID], [COMMENT_TIME]);
GO

CREATE INDEX [IDX_CMT_ROOT_COMMENT] ON [dbo].[COMMENTS] ([ROOT_COMMENT_ID]);
GO

CREATE INDEX [IDX_CMT_LANGUAGE] ON [dbo].[COMMENTS] ([COMMENT_LANGUAGE]);
GO

-- REPLIES
CREATE INDEX [IDX_RP_PARENT] ON [dbo].[REPLIES] ([PARENT_CMT_ID]);
GO

CREATE INDEX [IDX_RP_VIDEO] ON [dbo].[REPLIES] ([VIDEO_ID]);
GO

CREATE INDEX [IDX_RP_CREATOR_TIME] ON [dbo].[REPLIES] ([CREATOR_ID], [REPLY_TIME]);
GO

CREATE INDEX [IDX_RP_ROOT_COMMENT] ON [dbo].[REPLIES] ([ROOT_COMMENT_ID]);
GO

CREATE INDEX [IDX_RP_LANGUAGE] ON [dbo].[REPLIES] ([COMMENT_LANGUAGE]);
GO

-- CREATOR_TAGS
CREATE INDEX [IDX_CT_TAG] ON [dbo].[CREATOR_TAGS] ([TAG_ID]);
GO

-- CREATORS
CREATE INDEX [IDX_CR_SNAPSHOT_TIME] ON [dbo].[CREATORS] ([SNAPSHOT_TIME]);
GO

CREATE INDEX [IDX_CR_CATEGORY] ON [dbo].[CREATORS] ([CATEGORY]);
GO

-- ============================================================
-- 8. TRIGGER: tu dong cap nhat MISSING_PRICE_FLAG
-- ============================================================
CREATE TRIGGER [dbo].[TRG_PRICE_FLAG]
ON [dbo].[CREATORS]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE c
    SET [MISSING_PRICE_FLAG] = CASE WHEN c.[PRICE] IS NULL THEN 1 ELSE 0 END
    FROM [dbo].[CREATORS] c
    INNER JOIN inserted i
        ON c.[CREATOR_ID] = i.[CREATOR_ID];
END
GO

/*
================================================================
GHI CHU QUAN TRONG
================================================================

1. CAC FIELD MOI THEM TU COL_MEAN.XLSX

CREATORS:
- FOLLOWING_COUNT
- FRIEND_COUNT
- DIGG_COUNT
- VIDEO_COUNT
- CATEGORY
- RAW_JSON

VIDEOS:
- VQSCORE
- BITRATE
- CATEGORY_TYPE
- TITLE
- DESC
- MUSIC_TITLE
- MUSIC_AUTHOR
- RAW_JSON

COMMENTS:
- ROOT_COMMENT_ID
- COMMENT_LANGUAGE
- IS_HIGH_PURCHASE_INTENT
- CUSTOM_VERIFY
- FOLD_STATUS
- IS_AUTHOR_DIGGED
- LABEL_TEXTS
- NO_SHOW
- ENTERPRISE_VERIFY_REASON
- RELATIVE_USERS
- REPLY_SCORE
- SHOW_MORE_SCORE
- RAW_JSON
	[USER_UID]					NVARCHAR(50)   NULL,
	[USER_UNIQUE_ID]			NVARCHAR(100)  NULL,
REPLIES:
- ROOT_COMMENT_ID
- REPLY_COUNT
- COMMENT_LANGUAGE
- IS_HIGH_PURCHASE_INTENT
- CUSTOM_VERIFY
- FOLD_STATUS
- IS_AUTHOR_DIGGED
- LABEL_TEXTS
- NO_SHOW
- ENTERPRISE_VERIFY_REASON
- RELATIVE_USERS
- REPLY_SCORE
- SHOW_MORE_SCORE
- RAW_JSON
	[USER_UID]					NVARCHAR(50)   NULL,
	[USER_UNIQUE_ID]			NVARCHAR(100)  NULL,
2. CAC FIELD KHONG THEM VI DA TON TAI TU SCHEMA CU
- FOLLOWERS
- ENGAGEMENT
- MEDIAN_VIEWS
- TOTAL_LIKES
- COLLAB_SCORE
- PRICE
- MISSING_PRICE_FLAG
- SNAPSHOT_TIME
- VIDEO_ID
- CREATE_TIME
- VIEW_COUNT
- LIKE_COUNT
- COMMENT_COUNT
- SHARE_COUNT
- SAVE_COUNT
- COMMENT_ID
- COMMENT_TIME
- TEXT

3. VE PARENT_COMMENT_ID / ROOT_COMMENT_ID
- COMMENTS la comment goc, khong can PARENT_COMMENT_ID
- REPLIES dung PARENT_CMT_ID de tro den comment cha truc tiep
- ROOT_COMMENT_ID giup gom toan bo thread ve 1 comment goc

4. VE RAW_JSON
- Nen luu response goc de debug, audit, parse lai field moi sau nay
- Co the tach sang bang raw log rieng neu sau nay dung luong qua lon

================================================================
*/
