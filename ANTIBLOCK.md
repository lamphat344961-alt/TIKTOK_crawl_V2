# Hướng dẫn chống bị chặn khi crawl TikTok

Tài liệu này mô tả **bạn cần cung cấp gì** (nếu có) và **cách lấy từng thứ** để giảm tối đa nguy cơ bị TikTok chặn trong suốt quá trình crawl.

---

## 1. Những gì code đã tự làm (không cần bạn cung cấp)

- **Delay ngẫu nhiên** giữa mỗi request API (`config.py`: `DELAY_API_REQUEST`).
- **Retry** khi request lỗi (HTTP hoặc TikTok status), có nghỉ dài giữa các lần thử (`API_RETRY_TIMES`, `API_RETRY_BACKOFF`).
- **Pause định kỳ**: sau mỗi N request thì nghỉ thêm một đoạn dài (`PAUSE_EVERY_N_REQUESTS`, `PAUSE_DURATION`) để tránh pattern đều đặn.
- **Refresh session**: sau mỗi N video thì build lại session (lấy cookies mới từ Selenium) (`REFRESH_SESSION_EVERY_N_VIDEOS`).
- **Cookies & headers**: tự lấy từ Chrome đang mở trang TikTok (User-Agent, Referer, cookies như `msToken`, `tt_chain_token`).

Bạn chỉ cần **chỉnh config** (và tùy chọn proxy / thời gian chạy) như bên dưới.

---

## 2. Bạn cần cung cấp / quyết định

| # | Nội dung | Bắt buộc? | Ghi chú |
|---|----------|-----------|--------|
| 1 | **Tăng delay / giảm tốc** | Khuyến nghị | Chỉnh trong `config.py` (xem mục 3). |
| 2 | **Proxy** | Tùy chọn | Nếu crawl nhiều, nên dùng proxy (residential hoặc datacenter) để đổi IP. |
| 3 | **Thời gian chạy** | Khuyến nghị | Tránh chạy 24/7 liên tục; nên có khoảng nghỉ giữa các đợt. |
| 4 | **Tài khoản TikTok** | Đã dùng | Chrome profile đăng nhập TikTok sẵn — giữ 1 tài khoản “sạch”, tránh spam. |

---

## 3. Chỉnh config (không cần cung cấp file)

Mở **`config.py`** và chỉnh các block sau theo mức “an toàn” bạn muốn.

### 3.1. Tốc độ — chậm hơn = ít bị chặn hơn

```python
# Đang: (0.8, 1.5) giây giữa mỗi API call
DELAY_API_REQUEST  = (1.2, 2.5)   # tăng lên nếu vẫn bị lỗi

# Đang: (2.0, 3.5) giây sau khi chuyển video
DELAY_NEXT_VIDEO   = (3.0, 5.0)   # có thể tăng

# Nghỉ dài mỗi 20 request
PAUSE_EVERY_N_REQUESTS = 15       # giảm 15 = nghỉ thường hơn
PAUSE_DURATION         = (15.0, 40.0)  # nghỉ 15–40 giây
```

### 3.2. Retry khi bị lỗi / rate limit

```python
API_RETRY_TIMES    = 5            # thử tối đa 5 lần mỗi request
API_RETRY_BACKOFF  = (10.0, 30.0)  # nghỉ 10–30 giây giữa mỗi lần thử
```

### 3.3. Refresh session (cookies) mỗi N video

```python
# Refresh session sau mỗi 3 video (cookies mới từ Chrome)
REFRESH_SESSION_EVERY_N_VIDEOS = 3

# Đặt = None nếu muốn refresh mỗi video (chậm hơn nhưng an toàn)
# REFRESH_SESSION_EVERY_N_VIDEOS = None
```

### 3.4. Giới hạn số lượng (tránh “quá tải”)

```python
MAX_COMMENTS_PER_VIDEO  = 300   # giảm từ 500 nếu cần
MAX_REPLIES_PER_COMMENT = 100  # giảm từ 200 nếu cần
MAX_VIDEOS_PER_CREATOR  = 100  # giảm nếu 1 creator có quá nhiều video
```

---

## 4. Proxy (tùy chọn) — hướng dẫn cung cấp

Nếu bạn có proxy (HTTP/HTTPS), code sẽ dùng cho **mọi request API** (comment/list và comment/list/reply).

### 4.1. Bạn cần cung cấp

- **URL proxy** dạng: `http://host:port` hoặc `http://user:password@host:port`  
- Nếu proxy cần **user/pass**: đưa vào URL như trên, **không** ghi user/pass vào code dạng plain text trong repo (ưu tiên dùng biến môi trường).

### 4.2. Cách cấu hình

**Cách 1: Biến môi trường (khuyến nghị)**

1. Mở PowerShell (hoặc CMD).
2. Trước khi chạy `python main.py`, gõ:
   ```powershell
   $env:TIKTOK_PROXY = "http://user:pass@proxy.example.com:8080"
   python main.py
   ```
   Hoặc chỉ host:port:
   ```powershell
   $env:TIKTOK_PROXY = "http://127.0.0.1:7890"
   python main.py
   ```

**Cách 2: Sửa trong `config.py`**

Trong `config.py` tìm dòng:

```python
PROXY_URL = os.getenv("TIKTOK_PROXY", None)
```

Đổi thành (chỉ để test, tránh commit pass lên Git):

```python
PROXY_URL = os.getenv("TIKTOK_PROXY", "http://host:port")
```

### 4.3. Lấy proxy ở đâu (gợi ý)

- **Datacenter proxy**: các nhà cung cấp như Bright Data, Oxylabs, Smartproxy (trả phí).
- **Local proxy**: nếu bạn dùng VPN hoặc phần mềm proxy trên máy (VD: 127.0.0.1:7890), đặt `PROXY_URL = "http://127.0.0.1:7890"` (hoặc qua `TIKTOK_PROXY`).

Sau khi đặt `PROXY_URL` (qua env hoặc config), chạy lại crawl; log sẽ in `(proxy: on)` khi session khởi tạo.

---

## 5. Headers / cookies từ trình duyệt (khi cần debug)

Code **đã tự lấy** User-Agent, Referer, cookies từ Chrome (Selenium). Chỉ cần “bạn cung cấp” khi **debug** (VD: so sánh request thật vs request từ script).

### 5.1. Lấy request thật từ Chrome

1. Mở Chrome, đăng nhập TikTok, mở một video.
2. Nhấn **F12** → tab **Network**.
3. Lọc **Fetch/XHR** (hoặc gõ `comment/list` vào ô filter).
4. Kéo xuống comment (hoặc mở reply) để có request:
   - `comment/list/` (danh sách comment),
   - `comment/list/reply/` (danh sách reply).
5. Click vào từng request → tab **Headers**:
   - **Request URL**: full URL (có query).
   - **Request Headers**: `User-Agent`, `Referer`, cookie (hoặc copy cả cookie string).

### 5.2. Bạn có thể gửi cho người hỗ trợ (nếu cần)

- **Request URL** (full) của một lần gọi `comment/list` hoặc `comment/list/reply` (có thể xóa bớt query nhạy cảm, giữ `aweme_id` / `comment_id` / `cursor`).
- **Request Headers**: chụp màn hình hoặc copy text (User-Agent, Referer, Cookie) — **không gửi nếu có session đăng nhập thật**, chỉ khi cần so sánh format.

Code hiện tại **không** cần bạn paste headers vào file; chỉ dùng khi debug hoặc khi muốn bổ sung header đặc biệt (lúc đó cần sửa `comment_crawler._build_session()`).

---

## 6. Thời gian chạy (khuyến nghị)

- **Không** chạy 24/7 liên tục nhiều ngày.
- Nên:
  - Chạy vài giờ → dừng → nghỉ vài giờ (hoặc qua đêm) → chạy tiếp.
  - Hoặc giới hạn: ví dụ mỗi ngày crawl tối đa N creators rồi dừng (`MAX_CREATORS`).

---

## 7. Nếu vẫn bị chặn / lỗi

1. **Tăng delay và pause** (mục 3.1, 3.2).
2. **Giảm số lượng** (comments/replies/videos per creator) (mục 3.4).
3. **Bật proxy** (mục 4) và thử IP khác.
4. **Refresh session thường hơn**: `REFRESH_SESSION_EVERY_N_VIDEOS = 1` hoặc `None` (mỗi video build session mới).
5. **Đổi thời gian chạy**: nghỉ lâu hơn giữa các đợt (mục 6).
6. **Kiểm tra tài khoản**: đăng nhập bằng Chrome profile “sạch”, ít hành vi bất thường.

Nếu bạn gửi thêm thông tin (log lỗi, `status_code` / `status_msg` từ API, có dùng proxy hay không), có thể tinh chỉnh thêm từng bước cho phù hợp.
