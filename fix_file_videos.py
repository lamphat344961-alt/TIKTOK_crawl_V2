import csv

def normalize_videos_csv(input_path: str, output_path: str):
    """
    Đọc file CSV bất kỳ và chuẩn hóa về format:
    CREATOR_ID, VIDEO_ID
    """

    normalized_rows = []

    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        # Chuẩn hóa tên cột (lower + strip)
        field_map = {col.lower().strip(): col for col in reader.fieldnames}

        # Tìm cột tương ứng
        creator_col = None
        video_col = None

        for key in field_map:
            if key in ["creator_id", "creator", "id", "username"]:
                creator_col = field_map[key]
            if key in ["video_id", "video", "aweme_id", "id_video"]:
                video_col = field_map[key]

        if not creator_col or not video_col:
            raise ValueError(f"Không tìm thấy cột phù hợp. Columns hiện tại: {reader.fieldnames}")

        print(f"[INFO] Map cột: {creator_col} → CREATOR_ID | {video_col} → VIDEO_ID")

        # Đọc dữ liệu
        for row in reader:
            creator_id = str(row.get(creator_col, "")).strip()
            video_id = str(row.get(video_col, "")).strip()

            if not creator_id or not video_id:
                continue

            normalized_rows.append({
                "CREATOR_ID": creator_id,
                "VIDEO_ID": video_id
            })

    # Ghi file chuẩn
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["CREATOR_ID", "VIDEO_ID"])
        writer.writeheader()
        writer.writerows(normalized_rows)

    print(f"[DONE] Đã chuẩn hóa {len(normalized_rows)} dòng → {output_path}")


if __name__ == "__main__":
    normalize_videos_csv("videos.csv", "videos_fixed.csv")