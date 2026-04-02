"""
crawler/profile_feed_crawler.py
================================
Thay thế hoàn toàn creator_crawler.py + video_crawler.py.

Các trường hợp tài khoản không lấy được data:
  - not_found : username không tồn tại (404)
  - private   : tài khoản private
  - banned    : tài khoản bị ban / unavailable
  - no_videos : tồn tại nhưng không có video nào trong cửa sổ ngày crawl

Khi gặp các trường hợp trên, main.py sẽ set CRAWL_STATUS tương ứng
thay vì 'done', để dễ lọc và phân tích sau.

[PATCH] Bắt response đầu tiên sau khi user refresh vượt captcha:
  - crawl(): đọc buffer trước khi clear, merge vào all_items sau scroll
"""

from __future__ import annotations

import json
import time
import random
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

import crawler.config as config


# ===========================================================================
# JS HOOK
# ===========================================================================

_HOOK_JS = r"""
return (function () {
    if (window.__TT_HOOK_INSTALLED__) return "already_installed";
    window.__TT_HOOK_INSTALLED__ = true;
    window.__TT_CAPTURED__ = [];

    function safeJson(text) { try { return JSON.parse(text); } catch(e) { return null; } }
    function keep(url) { return url && url.includes("/api/post/item_list/"); }
    function push(e) { try { window.__TT_CAPTURED__.push(e); } catch(e) {} }

    const origFetch = window.fetch;
    window.fetch = async function(...args) {
        const resp = await origFetch.apply(this, args);
        try {
            const url = args[0] ? (typeof args[0] === "string" ? args[0] : args[0].url) : "";
            if (keep(url)) {
                const txt = await resp.clone().text();
                push({
                    url,
                    status: resp.status,
                    json: safeJson(txt),
                    capturedAt: new Date().toISOString()
                });
            }
        } catch(e) {}
        return resp;
    };

    const origOpen = XMLHttpRequest.prototype.open;
    const origSend = XMLHttpRequest.prototype.send;

    XMLHttpRequest.prototype.open = function(method, url) {
        this.__tt_url = url;
        return origOpen.apply(this, arguments);
    };

    XMLHttpRequest.prototype.send = function() {
        this.addEventListener("load", function() {
            try {
                if (!keep(this.__tt_url || "")) return;
                const txt = this.responseText || "";
                push({
                    url: this.__tt_url,
                    status: this.status,
                    json: safeJson(txt),
                    capturedAt: new Date().toISOString()
                });
            } catch(e) {}
        });
        return origSend.apply(this, arguments);
    };

    return "installed";
})();
"""

_GET_CAPTURED_JS = "return { count: (window.__TT_CAPTURED__||[]).length, data: window.__TT_CAPTURED__||[] };"
_CLEAR_JS = "window.__TT_CAPTURED__ = []; return true;"


# ===========================================================================
# ACCOUNT STATUS SIGNALS
# ===========================================================================

_NOT_FOUND_SIGNALS = [
    "couldn't find this account",
    "this account doesn't exist",
    "no longer available",
    "tài khoản này không tồn tại",
    "không tìm thấy tài khoản",
    "sorry, couldn't find",
]

_PRIVATE_SIGNALS = [
    "this account is private",
    "tài khoản này ở chế độ riêng tư",
    "follow to see their videos",
    "account is private",
]

_BANNED_SIGNALS = [
    "this account is not available",
    "account has been banned",
    "tài khoản này không khả dụng",
    "violated our community guidelines",
    "this user doesn't exist",
]


# ===========================================================================
# HELPERS
# ===========================================================================

def _wait_page(driver):
    WebDriverWait(driver, 30).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def _to_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _detect_account_status(driver) -> str | None:
    """
    Đọc body text để phát hiện tài khoản bất thường.
    Trả về: 'not_found' | 'private' | 'banned' | None
    """
    try:
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
    except Exception:
        return None

    for signal in _NOT_FOUND_SIGNALS:
        if signal in page_text:
            return "not_found"

    for signal in _PRIVATE_SIGNALS:
        if signal in page_text:
            return "private"

    for signal in _BANNED_SIGNALS:
        if signal in page_text:
            return "banned"

    return None


def _parse_captures(captures: list) -> dict[str, dict]:
    """
    Gộp toàn bộ itemList đã capture được thành map {video_id: raw_item}.
    Dedupe theo video_id.
    """
    items: dict[str, dict] = {}

    for entry in captures:
        payload = entry.get("json")
        if not isinstance(payload, dict):
            continue

        for item in (payload.get("itemList") or []):
            if isinstance(item, dict) and item.get("id"):
                items[str(item["id"])] = item

    return items


def _item_create_time_dt(item: dict) -> datetime | None:
    raw_ts = item.get("createTime")
    if raw_ts in (None, "", 0, "0"):
        return None
    try:
        return datetime.fromtimestamp(int(raw_ts))
    except Exception:
        return None


def _is_within_window(create_time_dt: datetime | None, days: int) -> bool:
    """
    Rule đã chốt:
      - parse được ngày -> giữ nếu trong window
      - không parse được ngày -> vẫn giữ
    """
    if create_time_dt is None:
        return True
    return create_time_dt >= datetime.now() - timedelta(days=days)


def _build_creator_dict(item: dict, tiktok_id: str) -> dict:
    """
    tiktok_id: username (ví dụ "lethikhanhhuyen2004") — là PK trong bảng CREATORS.
    KHÔNG dùng author.get("id") vì đó là numeric TikTok ID, không khớp với PK.
    """
    stats = item.get("authorStatsV2") or item.get("authorStats") or {}

    return {
        "CREATOR_ID": tiktok_id,          # ← username, nhất quán với CREATORS.CREATOR_ID
        "FOLLOWERS": _to_int(stats.get("followerCount")),
        "FOLLOWING_COUNT": _to_int(stats.get("followingCount")),
        "FRIEND_COUNT": _to_int(stats.get("friendCount")),
        "TOTAL_LIKES": _to_int(stats.get("heartCount") or stats.get("heart")),
        "DIGG_COUNT": _to_int(stats.get("diggCount")),
        "VIDEO_COUNT": _to_int(stats.get("videoCount")),
        "ENGAGEMENT": None,
        "MEDIAN_VIEWS": None,
        "SNAPSHOT_TIME": datetime.now(),
        "RAW_JSON": None,
    }


def _build_video_dict(item: dict, tiktok_id: str) -> dict:
    """
    tiktok_id: username — dùng làm CREATOR_ID để nhất quán với FK VIDEOS → CREATORS.
    KHÔNG dùng author.get("id") vì đó là numeric TikTok ID, sẽ vi phạm FK_VID_CR.
    """
    stats = item.get("statsV2") or item.get("stats") or {}
    music = item.get("music") or {}
    video = item.get("video") or {}

    anchor_types = None
    if item.get("AnchorTypes"):
        anchor_types = json.dumps(item["AnchorTypes"], ensure_ascii=False)

    create_time_dt = _item_create_time_dt(item)

    return {
        "VIDEO_ID": str(item.get("id") or ""),
        "CREATOR_ID": tiktok_id,          # ← username, nhất quán với CREATORS.CREATOR_ID
        "CREATE_TIME": create_time_dt,
        "ANCHOR_TYPES": anchor_types,
        "VIEW_COUNT": _to_int(stats.get("playCount")),
        "LIKE_COUNT": _to_int(stats.get("diggCount")),
        "COMMENT_COUNT": _to_int(stats.get("commentCount")),
        "SHARE_COUNT": _to_int(stats.get("shareCount")),
        "SAVE_COUNT": _to_int(stats.get("collectCount")),
        "VQSCORE": _to_float(video.get("VQScore")),
        "BITRATE": _to_int(video.get("bitrate")),
        "CATEGORY_TYPE": _to_int(item.get("CategoryType")),
        "TITLE": item.get("title"),
        "DESC": item.get("desc"),
        "MUSIC_TITLE": music.get("title"),
        "MUSIC_AUTHOR": music.get("authorName"),
        "MUSIC_PLAY_URL": music.get("playUrl"),
        "SNAPSHOT_TIME": datetime.now(),
        "RAW_JSON": None,
    }


# ===========================================================================
# MAIN CRAWLER CLASS
# ===========================================================================

class ProfileFeedCrawler:
    """
    Crawl profile feed creator TikTok bằng Firefox đã đăng nhập.
    Trả về:
      (creator_dict, [video_dict]) bình thường
      ({"CREATOR_ID": ..., "_FAIL_REASON": ...}, []) khi tài khoản có vấn đề
      None khi lỗi kỹ thuật
    """

    MAX_SCROLL_ROUNDS = 30
    MAX_IDLE_ROUNDS = 3

    # Timing cực đoan hơn nhưng vẫn còn vùng đệm để response kịp về
    PAGE_LOAD_WAIT = (1.2, 2.4)
    SCROLL_SLEEP_BASE = (0.8, 1.4)
    SCROLL_SLEEP_IDLE = (1.4, 2.2)

    # Rule dừng sớm theo yêu cầu
    OLD_VIDEO_STREAK_STOP = 3

    def __init__(self, driver):
        self.driver = driver

    # ------------------------------------------------------------------
    # PUBLIC
    # ------------------------------------------------------------------

    def crawl(self, tiktok_id: str, already_navigated: bool = False) -> tuple[dict, list[dict]] | None:
        """
        Crawl 1 creator.

        already_navigated=True: main.py đã gọi driver.get() và người dùng đã vượt
        captcha xong — KHÔNG load lại trang, chỉ đợi readyState rồi crawl luôn.

        Returns:
          (creator_dict, [video_dict])  — thành công bình thường
          ({"CREATOR_ID": id, "_FAIL_REASON": reason}, [])  — tài khoản có vấn đề
          None  — lỗi kỹ thuật
        """
        url = f"https://www.tiktok.com/@{tiktok_id}"
        print(f"\n[profile_feed] Crawl: {tiktok_id}  →  {url}")

        # ---- Load trang ----
        if not already_navigated:
            # Chế độ standalone: tự navigate
            try:
                self.driver.get(url)
                _wait_page(self.driver)
                time.sleep(random.uniform(*self.PAGE_LOAD_WAIT))
            except Exception as e:
                print(f"[profile_feed] Lỗi load trang {tiktok_id}: {e}")
                return None
        else:
            # Trang đã load bởi main.py (sau khi người dùng vượt captcha)
            # Chỉ đợi readyState + delay để TikTok kịp bắn request item_list đầu tiên
            try:
                _wait_page(self.driver)
                time.sleep(random.uniform(2.0, 3.5))
            except Exception as e:
                print(f"[profile_feed] Lỗi chờ trang {tiktok_id}: {e}")
                return None

        # ---- Detect tài khoản bất thường ----
        account_status = _detect_account_status(self.driver)
        if account_status:
            print(f"[profile_feed] @{tiktok_id} → {account_status.upper()}")
            return {"CREATOR_ID": tiktok_id, "_FAIL_REASON": account_status}, []

        # ---- [PATCH] Đọc buffer trước khi clear ----
        # Sau khi user vượt captcha và refresh trang, TikTok bắn response item_list
        # đầu tiên ngay khi load. Hook đã được re-inject trong main.py sau input(),
        # nên response này được bắt vào buffer. Phải đọc ra trước khi clear,
        # nếu không sẽ mất toàn bộ ~20 video đầu tiên.
        pre_items: dict[str, dict] = {}
        try:
            pre_captured = self.driver.execute_script(_GET_CAPTURED_JS)
            if isinstance(pre_captured, dict) and pre_captured.get("data"):
                pre_items = _parse_captures(pre_captured["data"])
                if pre_items:
                    print(f"[profile_feed] Pre-scroll buffer: {len(pre_items)} items (response đầu sau refresh)")
                else:
                    print(f"[profile_feed] Pre-scroll buffer: rỗng")
            else:
                print(f"[profile_feed] Pre-scroll buffer: rỗng")
        except Exception as e:
            print(f"[profile_feed] Lỗi đọc pre-scroll buffer: {e}")

        # Clear buffer rồi mới bắt đầu scroll
        self.driver.execute_script(_CLEAR_JS)
        print(f"[profile_feed] Buffer cleared, bắt đầu scroll")

        # ---- Scroll & collect raw items ----
        all_items = self._scroll_and_collect(tiktok_id)

        # ---- [PATCH] Merge pre_items vào all_items ----
        # pre_items chứa response đầu tiên (bị bỏ sót trước patch).
        # scroll thắng nếu trùng id (scroll có thể có data mới hơn).
        if pre_items:
            merged_count_before = len(all_items)
            all_items = {**pre_items, **all_items}
            gained = len(all_items) - merged_count_before
            print(f"[profile_feed] Sau merge pre-scroll: +{gained} items mới | tổng={len(all_items)}")

        if not all_items:
            print(f"[profile_feed] @{tiktok_id} → không lấy được item nào")
            return {"CREATOR_ID": tiktok_id, "_FAIL_REASON": "no_videos"}, []

        # ---- Build creator dict ----
        first_item = next(iter(all_items.values()))
        creator_dict = _build_creator_dict(first_item, tiktok_id)

        if not creator_dict.get("CREATOR_ID"):
            print(f"[profile_feed] Không parse được CREATOR_ID cho @{tiktok_id}")
            return None

        print(
            f"[profile_feed] CREATOR_ID={creator_dict['CREATOR_ID']} "
            f"| followers={creator_dict.get('FOLLOWERS')} "
            f"| videos_raw={len(all_items)}"
        )

        # ---- Final normalize videos ----
        days_window = getattr(config, "CRAWL_DAYS_WINDOW", 90)
        max_videos = getattr(config, "MAX_VIDEOS_PER_CREATOR", None)

        video_dicts, skipped_old, kept_unknown_time = self._finalize_videos(
            all_items=all_items,
            tiktok_id=tiktok_id,
            days_window=days_window,
            max_videos=max_videos,
        )

        print(
            f"[profile_feed] Videos giữ lại: {len(video_dicts)} "
            f"| bỏ cũ hơn {days_window} ngày: {skipped_old} "
            f"| giữ vì không parse được ngày: {kept_unknown_time}"
        )

        if not video_dicts:
            return {"CREATOR_ID": tiktok_id, "_FAIL_REASON": "no_videos"}, []

        return creator_dict, video_dicts

    # ------------------------------------------------------------------
    # PRIVATE: FINALIZE VIDEOS
    # ------------------------------------------------------------------

    def _finalize_videos(
        self,
        all_items: dict[str, dict],
        tiktok_id: str,
        days_window: int,
        max_videos: int | None,
    ) -> tuple[list[dict], int, int]:
        """
        Bước cuối:
          - sort mới -> cũ
          - chỉ build full video_dict nếu item cần giữ
          - item không parse được createTime vẫn giữ
          - cắt theo max_videos
        """
        sorted_items = sorted(
            all_items.values(),
            key=lambda x: int(x.get("createTime") or 0),
            reverse=True,
        )
        print(f"[DEBUG] Top 5 createTime của @{tiktok_id}:")
        for item in sorted_items[:5]:
            raw = item.get("createTime")
            dt = _item_create_time_dt(item)
            print(f"  id={item.get('id')}  raw_createTime={raw}  →  parsed={dt}")
        video_dicts: list[dict] = []
        skipped_old = 0
        kept_unknown_time = 0

        for item in sorted_items:
            video_id = str(item.get("id") or "")
            if not video_id:
                continue

            create_time_dt = _item_create_time_dt(item)
            within_window = _is_within_window(create_time_dt, days_window)

            if not within_window:
                skipped_old += 1
                continue

            if create_time_dt is None:
                kept_unknown_time += 1

            # Chỉ build full dict khi chắc chắn giữ
            v = _build_video_dict(item, tiktok_id)
            if not v["VIDEO_ID"]:
                continue

            video_dicts.append(v)

            if max_videos and len(video_dicts) >= max_videos:
                break

        return video_dicts, skipped_old, kept_unknown_time

    # ------------------------------------------------------------------
    # PRIVATE: SCROLL LOOP
    # ------------------------------------------------------------------

    def _scroll_and_collect(self, tiktok_id: str) -> dict[str, dict]:
        """
        Scroll mạnh 1 phát mỗi vòng, rồi đợi response.
        Dừng sớm theo:
          - đủ MAX_VIDEOS_PER_CREATOR video hợp lệ
          - gặp 3 video liên tiếp cũ hơn cửa sổ ngày
          - idle rounds
          - max scroll rounds

        Không dùng hard timeout collect.
        """
        days_window = getattr(config, "CRAWL_DAYS_WINDOW", 90)
        max_videos = getattr(config, "MAX_VIDEOS_PER_CREATOR", None)

        seen_ids: dict[str, dict] = {}
        idle_rounds = 0

        viewport_h = self.driver.execute_script("return window.innerHeight") or 800

        for round_idx in range(1, self.MAX_SCROLL_ROUNDS + 1):
            # ---- Scroll mạnh 1 phát mỗi vòng ----
            scroll_px = int(viewport_h * random.uniform(1.8, 2.4))
            self.driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_px)

            # ---- Chờ response ----
            if idle_rounds > 0:
                time.sleep(random.uniform(*self.SCROLL_SLEEP_IDLE))
            else:
                time.sleep(random.uniform(*self.SCROLL_SLEEP_BASE))

            # ---- Đọc buffer và clear ngay ----
            # BUG FIX: Phải clear buffer SAU KHI đọc, trước khi sang vòng tiếp.
            # Nếu không clear, mỗi vòng sẽ parse lại toàn bộ responses từ đầu,
            # khiến captured.count tăng dần và _parse_captures() tốn CPU ngày càng nhiều.
            # seen_ids.update() đã dedupe đúng nên số video cuối cùng không sai,
            # nhưng captures tích lũy vô hạn gây chậm và tốn RAM.
            captured = self.driver.execute_script(_GET_CAPTURED_JS)
            self.driver.execute_script(_CLEAR_JS)   # ← clear ngay sau khi đọc
            captures = captured.get("data", []) if isinstance(captured, dict) else []

            new_items = _parse_captures(captures)

            prev_count = len(seen_ids)
            seen_ids.update(new_items)
            gained = len(seen_ids) - prev_count

            enough_valid, old_streak = self._evaluate_stop_conditions(
                all_items=seen_ids,
                days_window=days_window,
                max_videos=max_videos,
            )

            print(
                f"[profile_feed] Round {round_idx:02d} | "
                f"responses={len(captures)} | "
                f"+{gained} items | total={len(seen_ids)} | "
                f"old_streak={old_streak}"
            )

            if gained > 0:
                idle_rounds = 0
            else:
                idle_rounds += 1

            if enough_valid:
                print(
                    f"[profile_feed] Đủ "
                    f"{max_videos if max_videos else 'số lượng yêu cầu'} video hợp lệ, dừng scroll"
                )
                break

            if old_streak >= self.OLD_VIDEO_STREAK_STOP:
                print(
                    f"[profile_feed] Gặp {old_streak} video liên tiếp cũ hơn "
                    f"{days_window} ngày, dừng scroll"
                )
                break

            if idle_rounds >= self.MAX_IDLE_ROUNDS:
                print(f"[profile_feed] {self.MAX_IDLE_ROUNDS} vòng idle, dừng scroll")
                break

        return seen_ids

    # ------------------------------------------------------------------
    # PRIVATE: STOP CONDITIONS
    # ------------------------------------------------------------------

    def _evaluate_stop_conditions(
        self,
        all_items: dict[str, dict],
        days_window: int,
        max_videos: int | None,
    ) -> tuple[bool, int]:
        """
        Đánh giá trên tập item đã merge/dedupe:
          - enough_valid: đã đủ max_videos video hợp lệ chưa
          - old_streak  : số video cũ liên tiếp tính trên danh sách sort mới -> cũ

        Lưu ý:
          - item không parse được ngày => giữ, không tăng old_streak
          - vì ưu tiên không bỏ sót video trong 90 ngày, old_streak chỉ tính sau khi đã
            sort trên toàn bộ tập item đang có, không tính kiểu raw-response ngay lập tức
        """
        sorted_items = sorted(
            all_items.values(),
            key=lambda x: int(x.get("createTime") or 0),
            reverse=True,
        )

        valid_count = 0
        old_streak = 0

        for item in sorted_items:
            create_time_dt = _item_create_time_dt(item)

            if create_time_dt is None:
                valid_count += 1
                old_streak = 0
            elif _is_within_window(create_time_dt, days_window):
                valid_count += 1
                old_streak = 0
            else:
                old_streak += 1

            if max_videos and valid_count >= max_videos:
                return True, old_streak

        return False, old_streak

    # ------------------------------------------------------------------
    # PUBLIC: cookies cho CommentCrawler
    # ------------------------------------------------------------------

    def get_cookies_for_requests(self) -> dict[str, str]:
        try:
            return {
                c["name"]: c["value"]
                for c in self.driver.get_cookies()
                if c.get("name")
            }
        except Exception as e:
            print(f"[profile_feed] Lỗi lấy cookies: {e}")
            return {}