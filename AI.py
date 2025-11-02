from google import genai
import mysql.connector
import time

# --- 1️⃣ Kết nối MySQL ---
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="YOUR_PASS_WORD",
    database="YOUR_DATABASE"
)
cursor = db.cursor()

# --- 2️⃣ Khởi tạo Gemini client ---
client = genai.Client(api_key="YOUR_API_KEY")

# --- 3️⃣ Lấy danh sách review chưa gán điểm ---
cursor.execute("SELECT review_id, comment, rating FROM reviews WHERE sentiment_score IS NULL")
reviews = cursor.fetchall()

# --- 4️⃣ Cấu hình batching ---
BATCH_SIZE = 20

# --- 5️⃣ Hàm đánh giá cảm xúc theo batch ---
def get_sentiment_scores(batch):
    batch_text = "\n".join(
        [f"Review {i+1}: \"{text}\" (Rating: {rating})"
         for i, (_, text, rating) in enumerate(batch)]
    )

    prompt = f"""
Bạn là mô hình đánh giá cảm xúc cho các review sản phẩm.

Dưới đây là {len(batch)} review:
{batch_text}

Yêu cầu:
- Với mỗi review, hãy trả về **duy nhất một con số** trong khoảng 1 đến 5.
- Cách cho điểm:
  + Nếu review_text trống hoặc vô nghĩa nhưng rating có giá trị → sentiment_score = rating.
  + Nếu là spam hoặc bình luận không liên quan → sentiment_score = 3.
  + Nếu có ngôn ngữ xúc phạm hoặc tiêu cực mạnh → sentiment_score = 1.
  + Nếu hợp lệ, hãy đánh giá cảm xúc dựa trên nội dung:
    * 1 = cực kỳ tiêu cực
    * 2 = tiêu cực
    * 3 = trung lập
    * 4 = tích cực
    * 5 = cực kỳ tích cực

Trả kết quả dưới dạng danh sách các số, mỗi dòng tương ứng một review:
Ví dụ:
3
4
2
5
...
Chỉ trả về danh sách số, không thêm ký tự nào khác.
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        lines = [l.strip() for l in response.text.strip().splitlines() if l.strip().isdigit()]
        scores = [int(l) for l in lines if 1 <= int(l) <= 5]
        return scores if len(scores) == len(batch) else None
    except Exception as e:
        print("Lỗi batch:", e)
        time.sleep(10)
        return None

# --- 6️⃣ Chạy theo batch ---
for i in range(0, len(reviews), BATCH_SIZE):
    batch = reviews[i:i+BATCH_SIZE]
    scores = get_sentiment_scores(batch)

    if scores:
        for (review_id, _, _), score in zip(batch, scores):
            cursor.execute("UPDATE reviews SET sentiment_score = %s WHERE review_id = %s", (score, review_id))
        db.commit()
        print(f"[+] Đã xử lý {len(batch)} review (từ {i+1} đến {i+len(batch)})")
    else:
        print(f"[!] Batch {i//BATCH_SIZE + 1} bị lỗi, bỏ qua hoặc thử lại.")

db.close()
print("✅ Hoàn tất phân tích cảm xúc (batch mode).")
