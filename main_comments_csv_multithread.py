from __future__ import annotations

"""
main_comments_csv_multithread.py
================================
Crawl TikTok comments + replies theo đa luồng và lưu ra CSV, không dùng DB.

Thiết kế:
- Input: 1 CSV chứa tối thiểu 2 cột: CREATOR_ID, VIDEO_ID
- Mỗi worker dùng requests.Session riêng
- Mỗi worker ghi ra file riêng để tránh race condition khi append CSV
- Có file status riêng để resume theo VIDEO_ID + CREATOR_ID
- Không deduplicate realtime; khuyến nghị merge + dedup hậu kỳ

Ví dụ chạy:
    python main_comments_csv_multithread.py \
        --input /path/to/videos.csv \
        --output /path/to/output/run_2026_03_25 \
        --workers 4

Input CSV tối thiểu:  
    CREATOR_ID,VIDEO_ID
    caonho,7614520477146565906
    maitrithuc2020,7604721559999155464
vào SQL : 
SELECT CREATOR_ID, VIDEO_ID
FROM VIDEOS
where CREATOR_ID not in (select CREATOR_ID from CREATORS where CRAWL_STATUS = 'done') -> seve as videos.csv rôi chạy file fix_file_videos.py 
rồi :
    chạy :  python main_comments_csv_multithread.py --input videos_fixed.csv --output output/run_1 --workers 8 --merge-after
"""

import argparse
import csv
import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from typing import Iterable

import requests

import config
from helpers import make_comment_id, make_reply_id


# ============================================================================
# API CONFIG
# ============================================================================
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.tiktok.com/",
    "origin": "https://www.tiktok.com",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

BASE_PARAMS = {
    "aid": "1988",
    "app_name": "tiktok_web",
}

API_COMMENT = getattr(config, "API_COMMENT_LIST", "https://www.tiktok.com/api/comment/list/")
API_REPLY = getattr(config, "API_REPLY_LIST", "https://www.tiktok.com/api/comment/list/reply/")

COMMENT_FIELDS = [
    "COMMENT_ID",
    "VIDEO_ID",
    "CREATOR_ID",
    "ROOT_COMMENT_ID",
    "COMMENT_TIME",
    "LIKE_COUNT",
    "REPLY_COUNT",
    "TEXT",
    "COMMENT_LANGUAGE",
    "IS_HIGH_PURCHASE_INTENT",
    "CUSTOM_VERIFY",
    "FOLD_STATUS",
    "IS_AUTHOR_DIGGED",
    "LABEL_TEXTS",
    "NO_SHOW",
    "ENTERPRISE_VERIFY_REASON",
    "RELATIVE_USERS",
    "REPLY_SCORE",
    "SHOW_MORE_SCORE",
    "RAW_JSON",
    "USER_UID",
    "USER_UNIQUE_ID",
    "SNAPSHOT_TIME",
]

REPLY_FIELDS = [
    "REPLY_ID",
    "PARENT_CMT_ID",
    "VIDEO_ID",
    "CREATOR_ID",
    "ROOT_COMMENT_ID",
    "REPLY_TIME",
    "LIKE_COUNT",
    "REPLY_COUNT",
    "TEXT",
    "COMMENT_LANGUAGE",
    "IS_HIGH_PURCHASE_INTENT",
    "CUSTOM_VERIFY",
    "FOLD_STATUS",
    "IS_AUTHOR_DIGGED",
    "LABEL_TEXTS",
    "NO_SHOW",
    "ENTERPRISE_VERIFY_REASON",
    "RELATIVE_USERS",
    "REPLY_SCORE",
    "SHOW_MORE_SCORE",
    "RAW_JSON",
    "USER_UID",
    "USER_UNIQUE_ID",
    "SNAPSHOT_TIME",
]

STATUS_FIELDS = [
    "CREATOR_ID",
    "VIDEO_ID",
    "WORKER_ID",
    "STARTED_AT",
    "FINISHED_AT",
    "STATUS",
    "COMMENTS_FETCHED",
    "COMMENTS_WRITTEN",
    "COMMENTS_WITH_REPLY",
    "REPLIES_FETCHED",
    "REPLIES_WRITTEN",
    "REQUEST_COUNT",
    "ERROR_MESSAGE",
]


# ============================================================================
# THREAD-SAFE LOGGING
# ============================================================================
_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


# ============================================================================
# CSV APPENDER
# ============================================================================
class CsvAppender:
    def __init__(self, path: Path, fieldnames: list[str]):
        self.path = path
        self.fieldnames = fieldnames
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames, extrasaction="ignore")
        if self.path.stat().st_size == 0:
            self._writer.writeheader()
            self._file.flush()

    def writerows(self, rows: Iterable[dict]) -> int:
        count = 0
        for row in rows:
            self._writer.writerow(row)
            count += 1
        self._file.flush()
        return count

    def writerow(self, row: dict) -> None:
        self._writer.writerow(row)
        self._file.flush()

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass


# ============================================================================
# TASK MODEL
# ============================================================================
@dataclass(frozen=True)
class VideoTask:
    creator_id: str
    video_id: str


# ============================================================================
# FETCHER
# ============================================================================
class TikTokCommentFetcher:
    def __init__(self, cookie_dict: dict[str, str] | None = None):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._req_count = 0
        if cookie_dict:
            self.session.cookies.update(cookie_dict)

        proxy_url = getattr(config, "PROXY_URL", None)
        if proxy_url:
            self.session.proxies.update({"http": proxy_url, "https": proxy_url})

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _maybe_pause(self) -> None:
        every = getattr(config, "PAUSE_EVERY_N_REQUESTS", 30)
        duration = getattr(config, "PAUSE_DURATION", (2.0, 5.0))
        if every > 0 and self._req_count > 0 and self._req_count % every == 0:
            wait = random.uniform(*duration)
            log(f"[fetcher] Pause định kỳ {wait:.1f}s (req #{self._req_count})")
            time.sleep(wait)

    def _get(self, url: str, params: dict, label: str = "") -> dict | None:
        retry_times = getattr(config, "API_RETRY_TIMES", 3)
        retry_backoff = getattr(config, "API_RETRY_BACKOFF", (5.0, 15.0))

        for attempt in range(1, retry_times + 1):
            try:
                self._req_count += 1
                resp = self.session.get(url, params=params, timeout=20)

                if resp.status_code != 200:
                    log(f"[fetcher] HTTP {resp.status_code} {label} -> bỏ qua")
                    return None

                if len(resp.content) == 0:
                    wait = random.uniform(*retry_backoff)
                    log(f"[fetcher] Body rỗng {label} -> retry {attempt}/{retry_times} sau {wait:.1f}s")
                    time.sleep(wait)
                    continue

                data = resp.json()
                if data.get("status_code") == 0:
                    return data

                log(
                    f"[fetcher] API status={data.get('status_code')} "
                    f"msg={data.get('status_msg', '')} {label}"
                )
                return None

            except requests.exceptions.RequestException as e:
                wait = random.uniform(*retry_backoff)
                log(f"[fetcher] Exception {label} lần {attempt}: {e} -> retry sau {wait:.1f}s")
                time.sleep(wait)
            except ValueError as e:
                log(f"[fetcher] JSON decode lỗi {label}: {e}")
                return None

        log(f"[fetcher] Hết retry: {label}")
        return None

    def fetch_all_comments(self, video_id: str) -> list[dict]:
        all_comments: list[dict] = []
        cursor = 0
        has_more = 1
        page = 0
        max_pages = max(1, config.MAX_COMMENTS_PER_VIDEO // config.API_COMMENT_COUNT)

        while has_more and page < max_pages:
            resp = self._get(
                API_COMMENT,
                {
                    **BASE_PARAMS,
                    "aweme_id": str(video_id),
                    "count": str(config.API_COMMENT_COUNT),
                    "cursor": str(cursor),
                },
                label=f"video={video_id} cursor={cursor}",
            )
            if not resp:
                break

            items = resp.get("comments") or []
            if not items:
                break

            all_comments.extend(items)
            cursor = resp.get("cursor", cursor + config.API_COMMENT_COUNT)
            has_more = resp.get("has_more", 0)
            page += 1

            if has_more:
                time.sleep(random.uniform(*config.DELAY_API_REQUEST))
            self._maybe_pause()

        return all_comments

    def fetch_all_replies(self, video_id: str, comment_id: str) -> list[dict]:
        all_replies: list[dict] = []
        cursor = 0
        has_more = 1
        page = 0
        max_pages = max(1, config.MAX_REPLIES_PER_COMMENT // config.API_REPLY_COUNT)

        while has_more and page < max_pages:
            resp = self._get(
                API_REPLY,
                {
                    **BASE_PARAMS,
                    "item_id": str(video_id),
                    "comment_id": str(comment_id),
                    "count": str(config.API_REPLY_COUNT),
                    "cursor": str(cursor),
                },
                label=f"reply={comment_id} page={page}",
            )
            if not resp:
                break

            replies = resp.get("comments") or []
            if not replies:
                break

            all_replies.extend(replies)
            cursor = resp.get("cursor", cursor + config.API_REPLY_COUNT)
            has_more = resp.get("has_more", 0)
            page += 1

            if has_more:
                time.sleep(random.uniform(*config.DELAY_API_REQUEST))
            self._maybe_pause()

        return all_replies


# ============================================================================
# NORMALIZATION
# ============================================================================
def _label_texts(label_list) -> str | None:
    if not label_list:
        return None
    texts: list[str] = []
    for item in label_list:
        if isinstance(item, dict):
            text = str(item.get("text") or "").strip()
            if text:
                texts.append(text)
    return "|".join(texts) if texts else None


def _json_dumps_or_none(obj) -> str | None:
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return None


def _to_csv_bool(value) -> int | None:
    if value is None:
        return None
    return 1 if bool(value) else 0


def normalize_comment_row(comment: dict, creator_id: str, video_id: str, include_raw_json: bool) -> dict:
    cid = str(comment.get("cid") or "").strip()
    comment_id = cid or make_comment_id(
        str(video_id),
        str(comment.get("create_time") or ""),
        comment.get("text") or "",
    )
    user_obj = comment.get("user") or {}
    sort_extra = comment.get("sort_extra_score") or {}
    now_iso = datetime.now().isoformat(timespec="seconds")

    row = {
        "COMMENT_ID": comment_id,
        "VIDEO_ID": str(video_id),
        "CREATOR_ID": str(creator_id),
        "ROOT_COMMENT_ID": comment.get("root_comment_id") or comment_id,
        "COMMENT_TIME": comment.get("create_time"),
        "LIKE_COUNT": comment.get("digg_count"),
        "REPLY_COUNT": comment.get("reply_comment_total"),
        "TEXT": comment.get("text"),
        "COMMENT_LANGUAGE": comment.get("comment_language"),
        "IS_HIGH_PURCHASE_INTENT": _to_csv_bool(comment.get("is_high_purchase_intent")),
        "CUSTOM_VERIFY": comment.get("custom_verify"),
        "FOLD_STATUS": comment.get("fold_status"),
        "IS_AUTHOR_DIGGED": _to_csv_bool(comment.get("is_author_digged")),
        "LABEL_TEXTS": _label_texts(comment.get("label_list")),
        "NO_SHOW": _to_csv_bool(comment.get("no_show")),
        "ENTERPRISE_VERIFY_REASON": comment.get("enterprise_verify_reason"),
        "RELATIVE_USERS": _json_dumps_or_none(comment.get("relative_users")),
        "REPLY_SCORE": sort_extra.get("reply_score"),
        "SHOW_MORE_SCORE": sort_extra.get("show_more_score"),
        "RAW_JSON": _json_dumps_or_none(comment) if include_raw_json else None,
        "USER_UID": str(user_obj.get("uid") or "") or None,
        "USER_UNIQUE_ID": user_obj.get("unique_id"),
        "SNAPSHOT_TIME": now_iso,
    }
    return row


def normalize_reply_row(reply: dict, creator_id: str, video_id: str, parent_comment_id: str, include_raw_json: bool) -> dict:
    cid = str(reply.get("cid") or "").strip()
    reply_id = cid or make_reply_id(
        str(parent_comment_id),
        str(reply.get("create_time") or ""),
        reply.get("text") or "",
    )
    user_obj = reply.get("user") or {}
    sort_extra = reply.get("sort_extra_score") or {}
    now_iso = datetime.now().isoformat(timespec="seconds")

    row = {
        "REPLY_ID": reply_id,
        "PARENT_CMT_ID": str(parent_comment_id),
        "VIDEO_ID": str(video_id),
        "CREATOR_ID": str(creator_id),
        "ROOT_COMMENT_ID": reply.get("root_comment_id") or str(parent_comment_id),
        "REPLY_TIME": reply.get("create_time"),
        "LIKE_COUNT": reply.get("digg_count"),
        "REPLY_COUNT": reply.get("reply_comment_total"),
        "TEXT": reply.get("text"),
        "COMMENT_LANGUAGE": reply.get("comment_language"),
        "IS_HIGH_PURCHASE_INTENT": _to_csv_bool(reply.get("is_high_purchase_intent")),
        "CUSTOM_VERIFY": reply.get("custom_verify"),
        "FOLD_STATUS": reply.get("fold_status"),
        "IS_AUTHOR_DIGGED": _to_csv_bool(reply.get("is_author_digged")),
        "LABEL_TEXTS": _label_texts(reply.get("label_list")),
        "NO_SHOW": _to_csv_bool(reply.get("no_show")),
        "ENTERPRISE_VERIFY_REASON": reply.get("enterprise_verify_reason"),
        "RELATIVE_USERS": _json_dumps_or_none(reply.get("relative_users")),
        "REPLY_SCORE": sort_extra.get("reply_score"),
        "SHOW_MORE_SCORE": sort_extra.get("show_more_score"),
        "RAW_JSON": _json_dumps_or_none(reply) if include_raw_json else None,
        "USER_UID": str(user_obj.get("uid") or "") or None,
        "USER_UNIQUE_ID": user_obj.get("unique_id"),
        "SNAPSHOT_TIME": now_iso,
    }
    return row


# ============================================================================
# INPUT / RESUME
# ============================================================================
def read_video_tasks(input_csv: Path) -> list[VideoTask]:
    if not input_csv.exists():
        raise FileNotFoundError(f"Không tìm thấy input CSV: {input_csv}")

    tasks: list[VideoTask] = []
    with open(input_csv, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"CREATOR_ID", "VIDEO_ID"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError("Input CSV phải có cột CREATOR_ID và VIDEO_ID")

        for row in reader:
            creator_id = str(row.get("CREATOR_ID") or "").strip()
            video_id = str(row.get("VIDEO_ID") or "").strip()
            if creator_id and video_id:
                tasks.append(VideoTask(creator_id=creator_id, video_id=video_id))
    return tasks


def load_done_keys(status_dir: Path) -> set[tuple[str, str]]:
    done_keys: set[tuple[str, str]] = set()
    if not status_dir.exists():
        return done_keys

    for path in status_dir.glob("video_status_worker_*.csv"):
        try:
            with open(path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    status = str(row.get("STATUS") or "").strip().lower()
                    creator_id = str(row.get("CREATOR_ID") or "").strip()
                    video_id = str(row.get("VIDEO_ID") or "").strip()
                    if status == "done" and creator_id and video_id:
                        done_keys.add((creator_id, video_id))
        except Exception as e:
            log(f"[resume] Không đọc được {path.name}: {e}")
    return done_keys


# ============================================================================
# WORKER
# ============================================================================
class CrawlWorker(threading.Thread):
    def __init__(
        self,
        worker_id: int,
        task_queue: Queue,
        output_dir: Path,
        include_raw_json: bool = False,
        cookie_dict: dict[str, str] | None = None,
    ):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.output_dir = output_dir
        self.include_raw_json = include_raw_json
        self.cookie_dict = cookie_dict or {}

        self.fetcher: TikTokCommentFetcher | None = None
        self.comments_csv: CsvAppender | None = None
        self.replies_csv: CsvAppender | None = None
        self.status_csv: CsvAppender | None = None

    def _open_outputs(self) -> None:
        wid = f"{self.worker_id:02d}"
        self.comments_csv = CsvAppender(
            self.output_dir / "comments" / f"comments_worker_{wid}.csv",
            COMMENT_FIELDS,
        )
        self.replies_csv = CsvAppender(
            self.output_dir / "replies" / f"replies_worker_{wid}.csv",
            REPLY_FIELDS,
        )
        self.status_csv = CsvAppender(
            self.output_dir / "status" / f"video_status_worker_{wid}.csv",
            STATUS_FIELDS,
        )
        self.fetcher = TikTokCommentFetcher(cookie_dict=self.cookie_dict)

    def _close_outputs(self) -> None:
        if self.comments_csv:
            self.comments_csv.close()
        if self.replies_csv:
            self.replies_csv.close()
        if self.status_csv:
            self.status_csv.close()
        if self.fetcher:
            self.fetcher.close()

    def run(self) -> None:
        self._open_outputs()
        try:
            while True:
                try:
                    task = self.task_queue.get(timeout=1)
                except Empty:
                    return

                try:
                    self._process_task(task)
                finally:
                    self.task_queue.task_done()
        finally:
            self._close_outputs()

    def _process_task(self, task: VideoTask) -> None:
        assert self.fetcher and self.comments_csv and self.replies_csv and self.status_csv

        started_at = datetime.now().isoformat(timespec="seconds")
        creator_id = task.creator_id
        video_id = task.video_id
        request_before = self.fetcher._req_count

        log(f"[worker-{self.worker_id:02d}] Start {creator_id} | {video_id}")

        comments_fetched = 0
        comments_written = 0
        comments_with_reply = 0
        replies_fetched = 0
        replies_written = 0
        final_status = "done"
        error_message = ""

        try:
            comments = self.fetcher.fetch_all_comments(video_id)
            comments_fetched = len(comments)

            comment_rows: list[dict] = []
            reply_rows: list[dict] = []

            for comment in comments:
                comment_row = normalize_comment_row(
                    comment=comment,
                    creator_id=creator_id,
                    video_id=video_id,
                    include_raw_json=self.include_raw_json,
                )
                comment_rows.append(comment_row)

            if comment_rows:
                comments_written = self.comments_csv.writerows(comment_rows)

            need_replies = [
                c for c in comments
                if int(c.get("reply_comment_total") or 0) > 0 and str(c.get("cid") or "").strip()
            ]
            comments_with_reply = len(need_replies)

            for idx, comment in enumerate(need_replies, start=1):
                parent_comment_id = str(comment.get("cid") or "").strip()
                replies = self.fetcher.fetch_all_replies(video_id, parent_comment_id)
                replies_fetched += len(replies)

                for reply in replies:
                    reply_rows.append(
                        normalize_reply_row(
                            reply=reply,
                            creator_id=creator_id,
                            video_id=video_id,
                            parent_comment_id=parent_comment_id,
                            include_raw_json=self.include_raw_json,
                        )
                    )

                if idx < len(need_replies):
                    time.sleep(random.uniform(*config.DELAY_API_REQUEST))

            if reply_rows:
                replies_written = self.replies_csv.writerows(reply_rows)

        except Exception as e:
            final_status = "error"
            error_message = str(e)
            log(f"[worker-{self.worker_id:02d}] ERROR {creator_id} | {video_id} | {e}")

        finished_at = datetime.now().isoformat(timespec="seconds")
        request_after = self.fetcher._req_count
        request_used = max(0, request_after - request_before)

        self.status_csv.writerow({
            "CREATOR_ID": creator_id,
            "VIDEO_ID": video_id,
            "WORKER_ID": self.worker_id,
            "STARTED_AT": started_at,
            "FINISHED_AT": finished_at,
            "STATUS": final_status,
            "COMMENTS_FETCHED": comments_fetched,
            "COMMENTS_WRITTEN": comments_written,
            "COMMENTS_WITH_REPLY": comments_with_reply,
            "REPLIES_FETCHED": replies_fetched,
            "REPLIES_WRITTEN": replies_written,
            "REQUEST_COUNT": request_used,
            "ERROR_MESSAGE": error_message,
        })

        log(
            f"[worker-{self.worker_id:02d}] Done {creator_id} | {video_id} | "
            f"status={final_status} | c={comments_written}/{comments_fetched} | "
            f"r={replies_written}/{replies_fetched} | req={request_used}"
        )


# ============================================================================
# MERGE / DEDUP
# ============================================================================
def merge_and_dedup_csvs(output_dir: Path) -> None:
    merged_dir = output_dir / "merged"
    merged_dir.mkdir(parents=True, exist_ok=True)

    merge_specs = [
        (
            output_dir / "comments",
            "comments_worker_*.csv",
            merged_dir / "comments_merged_dedup.csv",
            COMMENT_FIELDS,
            ("COMMENT_ID", "VIDEO_ID", "CREATOR_ID"),
        ),
        (
            output_dir / "replies",
            "replies_worker_*.csv",
            merged_dir / "replies_merged_dedup.csv",
            REPLY_FIELDS,
            ("REPLY_ID", "PARENT_CMT_ID", "VIDEO_ID", "CREATOR_ID"),
        ),
        (
            output_dir / "status",
            "video_status_worker_*.csv",
            merged_dir / "video_status_merged.csv",
            STATUS_FIELDS,
            None,
        ),
    ]

    for src_dir, pattern, out_path, fieldnames, dedup_keys in merge_specs:
        files = sorted(src_dir.glob(pattern))
        if not files:
            continue

        seen = set()
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()

            total_in = 0
            total_out = 0
            for file in files:
                with open(file, "r", newline="", encoding="utf-8-sig") as f_in:
                    reader = csv.DictReader(f_in)
                    for row in reader:
                        total_in += 1
                        if dedup_keys:
                            key = tuple(str(row.get(k) or "").strip() for k in dedup_keys)
                            if key in seen:
                                continue
                            seen.add(key)
                        writer.writerow(row)
                        total_out += 1

        log(f"[merge] {out_path.name}: in={total_in}, out={total_out}")


# ============================================================================
# COOKIE SUPPORT (optional)
# ============================================================================
def load_cookie_dict(cookie_json_path: str | None) -> dict[str, str]:
    if not cookie_json_path:
        return {}

    path = Path(cookie_json_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy cookie file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items()}

    if isinstance(data, list):
        out: dict[str, str] = {}
        for item in data:
            if isinstance(item, dict) and item.get("name"):
                out[str(item["name"])] = str(item.get("value", ""))
        return out

    raise ValueError("Cookie JSON phải là dict {name:value} hoặc list cookie objects")


# ============================================================================
# MAIN
# ============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl TikTok comments/replies đa luồng ra CSV")
    parser.add_argument("--input", required=True, help="Path tới input CSV có cột CREATOR_ID, VIDEO_ID")
    parser.add_argument("--output", required=True, help="Thư mục output của 1 lần chạy")
    parser.add_argument("--workers", type=int, default=3, help="Số worker threads")
    parser.add_argument("--limit", type=int, default=0, help="Giới hạn số video tasks, 0 = không giới hạn")
    parser.add_argument("--include-raw-json", action="store_true", help="Lưu RAW_JSON vào CSV")
    parser.add_argument("--cookie-json", default="", help="Path cookie JSON tùy chọn")
    parser.add_argument("--merge-after", action="store_true", help="Merge + dedup CSV sau khi crawl xong")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_csv = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "comments").mkdir(parents=True, exist_ok=True)
    (output_dir / "replies").mkdir(parents=True, exist_ok=True)
    (output_dir / "status").mkdir(parents=True, exist_ok=True)

    cookie_dict = load_cookie_dict(args.cookie_json) if args.cookie_json else {}

    tasks = read_video_tasks(input_csv)
    if args.limit and args.limit > 0:
        tasks = tasks[:args.limit]

    done_keys = load_done_keys(output_dir / "status")
    pending_tasks = [t for t in tasks if (t.creator_id, t.video_id) not in done_keys]

    log(f"[main] input tasks      : {len(tasks)}")
    log(f"[main] done tasks       : {len(done_keys)}")
    log(f"[main] pending tasks    : {len(pending_tasks)}")
    log(f"[main] workers          : {args.workers}")
    log(f"[main] output           : {output_dir}")
    log(f"[main] include_raw_json : {args.include_raw_json}")
    log(f"[main] cookies loaded   : {len(cookie_dict)}")

    if not pending_tasks:
        log("[main] Không còn video nào cần crawl.")
        if args.merge_after:
            merge_and_dedup_csvs(output_dir)
        return

    q: Queue = Queue()
    for task in pending_tasks:
        q.put(task)

    workers: list[CrawlWorker] = []
    started_at = time.time()

    for i in range(1, args.workers + 1):
        worker = CrawlWorker(
            worker_id=i,
            task_queue=q,
            output_dir=output_dir,
            include_raw_json=args.include_raw_json,
            cookie_dict=cookie_dict,
        )
        worker.start()
        workers.append(worker)

    q.join()
    for worker in workers:
        worker.join(timeout=2)

    elapsed = time.time() - started_at
    log(f"[main] Done. Elapsed = {elapsed:.2f}s")

    if args.merge_after:
        merge_and_dedup_csvs(output_dir)


if __name__ == "__main__":
    main()
