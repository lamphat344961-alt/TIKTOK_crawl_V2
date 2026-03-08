"""
test_minimal_params.py
======================
Test comment_crawler mới (no-cookie mode).
Chạy: python test_minimal_params.py
"""

import sys, os
sys.path.insert(0, ".")

# Patch config nếu chưa có
import config

VIDEO_ID   = "7614520477146565906"
CREATOR_ID = "test_creator"


class DryRunDB:
    def get_existing_comment_cids(self, video_id):         return set()
    def get_existing_reply_cids_for_comment(self, cid):    return set()
    def upsert_comments(self, comments, creator_id, video_id, skip_cids=None):
        print(f"\n  [DryRun] {len(comments)} comments nhận được:")
        for i, c in enumerate(comments[:5], 1):
            user  = c.get("user", {}).get("nickname", "?")
            text  = (c.get("text") or "")[:60]
            likes = c.get("digg_count", 0)
            reps  = c.get("reply_comment_total", 0)
            print(f"    [{i}] @{user}: {text}")
            print(f"         ❤️{likes}  💬{reps} replies")
        if len(comments) > 5:
            print(f"    ... và {len(comments)-5} comments nữa")
        return len(comments)
    def upsert_replies(self, replies, creator_id, video_id, parent_comment_id, skip_cids=None):
        print(f"  [DryRun] {len(replies)} replies cho comment {parent_comment_id}")
        return len(replies)
    def close(self): pass


from crawler.comment_crawler import CommentCrawler

print("=" * 60)
print(f"  TEST: comment_crawler (no-cookie mode)")
print(f"  Video: {VIDEO_ID}")
print("=" * 60)

crawler = CommentCrawler(DryRunDB())
result  = crawler.crawl(CREATOR_ID, VIDEO_ID)

print("\n" + "=" * 60)
print(f"  Comments : {result['total_comments']}")
print(f"  Replies  : {result['total_replies']}")
if result["total_comments"] > 0:
    print("  ✅ HOẠT ĐỘNG — sẵn sàng áp vào main.py")
else:
    print("  ❌ Vẫn lỗi")
print("=" * 60)