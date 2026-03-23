# 📊 TikTok Creators Clustering – Data Processing & Sampling Report

---

## 1. 🎯 Mục tiêu

Xây dựng pipeline xử lý dữ liệu và sampling để phục vụ bài toán **Clustering Vietnamese TikTok Creators** trên dữ liệu đa chiều.

**Mục tiêu cụ thể:**

- Làm sạch và chuẩn hóa dữ liệu thô
- Phân tích phân phối dữ liệu (EDA)
- Xây dựng 2 chiến lược sampling:
  - Hướng 1: Data Quality
  - Hướng 2: Market Representation
- Tạo dataset 3000 creators phục vụ clustering

---

## 2. 🧱 Tổng quan dữ liệu

- **Tổng số creators:** 9,966
- Không có duplicate `creator_id`
- Dữ liệu ban đầu ở dạng `object` (string) → cần parse sang numeric

**Các feature chính:**

| Feature | Mô tả |
|---|---|
| `followers` | Số lượng người theo dõi |
| `engagement` | Tỷ lệ tương tác |
| `median_views` | Lượt xem trung vị |
| `price` | Giá hợp tác |
| `broadcast_score` | Điểm broadcast |
| `collab_score` | Điểm hợp tác |
| `category` | Danh mục nội dung |

---

## 3. 🧹 Data Cleaning & Feature Engineering

### 3.1. Chuẩn hóa dữ liệu

Các cột được chuyển sang numeric:

- `followers_num`
- `engagement_num`
- `median_views_num`
- `price_num`
- `broadcast_score_num`
- `collab_score_num`

### 3.2. Tạo feature bổ sung

| Feature | Logic |
|---|---|
| `size_tier` | MICRO: < 500K / MID: 500K–2M / MACRO: ≥ 2M |
| `price_tier` | LOW / MID / HIGH (theo percentile) |
| `primary_category_group` | Gom nhóm category |
| `n_categories` | Số category mỗi creator |

### 3.3. Log transform (giảm skew)

Áp dụng cho:

- `followers_num` → `followers_num_log1p`
- `median_views_num` → `median_views_num_log1p`
- `price_num` → `price_num_log1p`

---

## 4. 🔍 Phân tích dữ liệu (EDA)

### 4.1. Missing Values

| Feature | Missing |
|---|---|
| `price_num` | 58.8% |
| `broadcast_score_num` | 59.7% |
| `collab_score_num` | 1.8% |
| `engagement_num` | 0.23% |
| `median_views_num` | 0.23% |

> 💡 **Insight:** Dataset bị chia thành 2 nhóm — có price (~41%) và không có price (~59%).

### 4.2. Outliers

| Feature | Số outliers |
|---|---|
| `followers` | 1,103 |
| `views` | 1,133 |
| `price` | 435 |
| `engagement` | 425 |

> ⚠️ Đây là **heavy-tail distribution**, không phải noise.

### 4.3. Phân phối `size_tier`

| Tier | Count | % |
|---|---|---|
| MICRO | 8,554 | 85.8% |
| MID | 1,139 | 11.4% |
| MACRO | 273 | 2.7% |

> 💡 Long-tail distribution rất rõ.

### 4.4. Phân phối category

| Category | % |
|---|---|
| Others | 29% |
| Entertainment | 27% |
| Lifestyle | 18% |
| Beauty & Fashion | 14% |
| Knowledge / Tech | 9% |

> 💡 Phân phối khá cân bằng.

### 4.5. Cross-tab insights

**Size × Price:**
- MACRO → chủ yếu HIGH
- MICRO → nhiều missing
> Phản ánh đúng thị trường.

**Size × Category:**
- MICRO dominate mọi category
- MACRO phân bố đều

---

## 5. 🧪 Sampling Strategy

### 5.1. Hướng 1 – Data Quality *(Khuyên dùng)*

**Logic:** Lọc creators có đủ `price_num`, `engagement_num`, `median_views_num`.

| Chỉ số | Giá trị |
|---|---|
| Quality pool | 4,101 creators (~41%) |
| Sample | 3,000 creators |

**Đặc điểm:**
- Data đầy đủ
- Không cần imputation

**⚠️ Nhược điểm:** Distribution bị skew (MICRO ~82%, MID ~14%, MACRO ~4%) → không phản ánh đúng market.

---

### 5.2. Hướng 2 – Market Stratified Sampling

**Logic:**

1. **Bước 1:** Loại Unlabeled
2. **Bước 2:** Giữ toàn bộ MACRO (~269 creators)
3. **Bước 3:** Sampling phần còn lại (2,731 creators)
4. **Bước 4:** Stratify theo `(size_tier × primary_category_group)` với quota allocation

**Ví dụ quota:**

| Group | Quota |
|---|---|
| MICRO + Entertainment | 690 |
| MICRO + Others | 667 |
| MICRO + Lifestyle | 465 |
| ... | ... |

**Kết quả:** `market_sample = 3,000 creators`, không duplicate, phân phối hợp lý.

---

## 6. 📈 Validation kết quả

### 6.1. Size tier

| Tier | Sample | Source | Diff |
|---|---|---|---|
| MICRO | 80.3% | 85.8% | -5.5% |
| MID | 10.6% | 11.3% | -0.7% |
| MACRO | 8.9% | 2.7% | +6.2% |

> 💡 MACRO được **oversample** có chủ đích.

### 6.2. Category distribution

Sai lệch rất nhỏ:

| Category | Diff |
|---|---|
| Others | +1.3% |
| Entertainment | -0.6% |
| Lifestyle | -0.3% |
| Beauty | -0.25% |

> ✅ Gần như giữ nguyên distribution.

### 6.3. Stratum-level validation

Sai lệch cực nhỏ: ~ -1% → -0.1%

> ✅ Stratification hoạt động rất tốt.

---

## 7. 🧠 Insight quan trọng

### 7.1. Dataset có 2 bản chất
- **Economic space** (có price)
- **Content space** (toàn bộ)

### 7.2. Sampling ảnh hưởng trực tiếp đến clustering
- Random → collapse cluster
- Stratified → meaningful clusters

### 7.3. MACRO là điểm neo (anchor)
- Giúp xác định boundary của data space
- Rất quan trọng cho clustering

### 7.4. Không nên chọn 1 hướng
Nên dùng **cả 2**:
- **Hướng 1** → clustering theo giá trị thực
- **Hướng 2** → clustering theo thị trường

---

## 8. 🚀 Kết luận

Pipeline đạt:

| Tiêu chí | Trạng thái |
|---|---|
| Data clean | ✔ |
| Feature hợp lý | ✔ |
| Sampling robust | ✔ |
| Không bias | ✔ |
| Reproducible | ✔ |
| Sẵn sàng cho clustering | ✔ |

**Đóng góp chính:**
- Xử lý imbalance đúng cách
- Thiết kế stratified sampling đa chiều
- Kết hợp data quality & market view