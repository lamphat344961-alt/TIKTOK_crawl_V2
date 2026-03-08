"""
test_api_deep.py
================
Test nhiều cách gọi API khác nhau để tìm ra cách nào hoạt động.
Thử lần lượt từng approach, dừng lại khi thành công.

Chạy: python test_api_deep.py
"""

import json, time, random
from pathlib import Path
import requests

COOKIES_FILE = "tiktok_cookies.json"
VIDEO_ID     = "7614520477146565906"

# ===========================================================================
def load_cookies() -> dict:
    raw = json.loads(Path(COOKIES_FILE).read_text(encoding="utf-8"))
    return {c["name"]: c["value"] for c in raw if c.get("value")}

def check(resp) -> bool:
    """In kết quả, trả về True nếu thành công."""
    new_token = resp.headers.get("x-ms-token", "")
    print(f"  Status: {resp.status_code} | Body: {len(resp.content)} bytes | x-ms-token: {'YES' if new_token else 'NO'}")
    if len(resp.content) == 0:
        print("  → ❌ Body rỗng")
        return False
    try:
        data = resp.json()
        sc   = data.get("status_code")
        msg  = data.get("status_msg", "")
        cmts = data.get("comments") or []
        print(f"  → status_code={sc} | msg={msg} | comments={len(cmts)} | total={data.get('total',0)}")
        if sc == 0:
            print("  → ✅ THÀNH CÔNG!")
            if cmts:
                u = cmts[0].get("user",{}).get("nickname","?")
                t = (cmts[0].get("text") or "")[:60]
                print(f"     Comment[0]: @{u}: {t}")
            return True
        return False
    except Exception as e:
        body_preview = resp.text[:200]
        print(f"  → ❌ JSON lỗi: {e}")
        print(f"     Body: {repr(body_preview)}")
        return False

def sep(title=""):
    print(f"\n{'='*60}")
    if title: print(f"  {title}")

# ===========================================================================
cookies  = load_cookies()
ms_token = cookies.get("msToken","")
session_id = cookies.get("sessionid","")
print(f"Cookies loaded: {len(cookies)} items")
print(f"sessionid: {session_id[:15]}...")
print(f"msToken  : {ms_token[:30]}...")

# ===========================================================================
# TEST 1: Dùng mobile API endpoint (khác web)
# ===========================================================================
sep("TEST 1: Mobile API endpoint")
try:
    s = requests.Session()
    s.headers.update({
        "user-agent": "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 10; en_US; Pixel 4; Build/QQ3A.200805.001; Cronet/58.0.2991.0)",
        "accept": "application/json",
    })
    s.cookies.update(cookies)
    resp = s.get(
        "https://api16-normal-c-useast1a.tiktokv.com/aweme/v1/comment/list/",
        params={"aweme_id": VIDEO_ID, "count": "20", "cursor": "0"},
        timeout=15,
    )
    check(resp)
except Exception as e:
    print(f"  → Exception: {e}")

time.sleep(2)

# ===========================================================================
# TEST 2: Web API không có msToken
# ===========================================================================
sep("TEST 2: Web API không có msToken")
try:
    s = requests.Session()
    s.headers.update({
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "referer": f"https://www.tiktok.com/@user/video/{VIDEO_ID}",
    })
    s.cookies.update(cookies)
    resp = s.get(
        "https://www.tiktok.com/api/comment/list/",
        params={"aweme_id": VIDEO_ID, "count": "20", "cursor": "0",
                "aid": "1988", "app_name": "tiktok_web"},
        timeout=15,
    )
    check(resp)
except Exception as e:
    print(f"  → Exception: {e}")

time.sleep(2)

# ===========================================================================
# TEST 3: Web API với X-Bogus param (TikTok đôi khi yêu cầu)
# ===========================================================================
sep("TEST 3: Web API + thêm các params mới")
try:
    s = requests.Session()
    s.headers.update({
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "accept-language": "vi-VN,vi;q=0.9,en;q=0.8",
        "referer": f"https://www.tiktok.com/@user/video/{VIDEO_ID}",
        "origin": "https://www.tiktok.com",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    })
    s.cookies.update(cookies)
    resp = s.get(
        "https://www.tiktok.com/api/comment/list/",
        params={
            "WebIdLastTime": "1765616912",
            "aid": "1988",
            "app_language": "vi",
            "app_name": "tiktok_web",
            "aweme_id": VIDEO_ID,
            "browser_language": "vi-VN",
            "browser_name": "Mozilla",
            "browser_online": "true",
            "browser_platform": "Win32",
            "channel": "tiktok_web",
            "cookie_enabled": "true",
            "count": "20",
            "cursor": "0",
            "device_id": "7581340761837273096",
            "device_platform": "web_pc",
            "from_page": "video",
            "msToken": ms_token,
            "os": "windows",
            "priority_region": "",
            "region": "VN",
            "tz_name": "Asia/Ho_Chi_Minh",
        },
        timeout=15,
    )
    check(resp)
    new_token = resp.headers.get("x-ms-token","")
    if new_token:
        ms_token = new_token
        print(f"  Token rotated → {ms_token[:30]}...")
except Exception as e:
    print(f"  → Exception: {e}")

time.sleep(2)

# ===========================================================================
# TEST 4: Dùng token mới (nếu đã rotate ở test 3)
# ===========================================================================
sep("TEST 4: Repeat với token đã rotate")
try:
    s = requests.Session()
    s.headers.update({
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "accept-language": "vi-VN,vi;q=0.9",
        "referer": f"https://www.tiktok.com/@user/video/{VIDEO_ID}",
        "origin": "https://www.tiktok.com",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    })
    s.cookies.update(cookies)
    s.cookies.set("msToken", ms_token, domain=".tiktok.com")
    resp = s.get(
        "https://www.tiktok.com/api/comment/list/",
        params={
            "aid": "1988", "app_name": "tiktok_web",
            "aweme_id": VIDEO_ID, "count": "20", "cursor": "0",
            "device_platform": "web_pc", "os": "windows",
            "msToken": ms_token,
            "region": "VN", "tz_name": "Asia/Ho_Chi_Minh",
        },
        timeout=15,
    )
    check(resp)
except Exception as e:
    print(f"  → Exception: {e}")

time.sleep(2)

# ===========================================================================
# TEST 5: Không dùng cookies chút nào (public API)
# ===========================================================================
sep("TEST 5: Không cookies (public access)")
try:
    s = requests.Session()
    s.headers.update({
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "referer": f"https://www.tiktok.com/@user/video/{VIDEO_ID}",
    })
    resp = s.get(
        "https://www.tiktok.com/api/comment/list/",
        params={"aweme_id": VIDEO_ID, "count": "20", "cursor": "0", "aid": "1988"},
        timeout=15,
    )
    check(resp)
except Exception as e:
    print(f"  → Exception: {e}")

time.sleep(2)

# ===========================================================================
# TEST 6: TikTok oEmbed / embed API (không cần auth)
# ===========================================================================
sep("TEST 6: TikTok embed page scrape")
try:
    s = requests.Session()
    s.headers.update({
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "accept": "text/html,application/xhtml+xml",
    })
    resp = s.get(
        f"https://www.tiktok.com/embed/v2/{VIDEO_ID}",
        timeout=15,
    )
    print(f"  Status: {resp.status_code} | Body: {len(resp.content)} bytes")
    if "comment" in resp.text.lower():
        print("  → Có chứa từ 'comment' trong embed page")
    else:
        print("  → Không có comment data trong embed")
except Exception as e:
    print(f"  → Exception: {e}")

# ===========================================================================
print(f"\n{'='*60}")
print("  KẾT LUẬN: xem test nào in ✅ ở trên")
print(f"{'='*60}")