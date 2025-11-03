# visualize.py
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import re

# ==============================
# 1️⃣ Kết nối MySQL và đọc dữ liệu
# ==============================
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="25251325",  # giống file main.py
    database="review_crawl"
)

# Đọc bảng sản phẩm và đánh giá
products_df = pd.read_sql("SELECT * FROM products", conn)
reviews_df = pd.read_sql("SELECT * FROM reviews", conn)

conn.close()

print("✅ Đã lấy dữ liệu thành công!")
print(f"Sản phẩm: {products_df.shape}  |  Đánh giá: {reviews_df.shape}")

# ==============================
# 2️⃣ Xử lý dữ liệu bằng Pandas
# ==============================

def parse_sold(s):
    """Chuyển chuỗi 'đã bán' thành số (ví dụ: '1,2k+' -> 1200)."""
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.lower().replace("đã bán", "").replace(",", "").replace("+", "").strip()

    if "k" in s:
        try:
            return float(s.replace("k", "")) * 1000
        except:
            return None

    try:
        s_clean = re.sub(r"\D", "", s)
        return int(s_clean) if s_clean else None
    except:
        return None

# Thêm cột sold_num
products_df["sold_num"] = products_df["sold"].apply(parse_sold)

# Tính điểm trung bình và số lượng review cho mỗi sản phẩm
rating_summary = reviews_df.groupby("product_id").agg(
    avg_rating=("rating", "mean"),
    review_count=("rating", "count")
).reset_index()

# Gộp với bảng sản phẩm
merged_df = pd.merge(products_df, rating_summary, on="product_id", how="left")

# Điền giá trị mặc định nếu thiếu
merged_df["avg_rating"] = merged_df["avg_rating"].fillna(0)
merged_df["review_count"] = merged_df["review_count"].fillna(0)
merged_df["sold_num"] = merged_df["sold_num"].fillna(0)

# ==============================
# 3️⃣ Vẽ biểu đồ
# ==============================

# ✅ Hàm tiện ích rút gọn tên quá dài
def shorten(name, max_len=60):
    return name if len(name) <= max_len else name[:max_len] + "..."

# ========== Biểu đồ 1 ==========
top_rated = merged_df.sort_values("avg_rating", ascending=False).head(10)
plt.figure(figsize=(10, 6))
bars = plt.barh(
    [shorten(x) for x in top_rated["title"]],
    top_rated["avg_rating"],
    color="#2ecc71"
)
plt.xlabel("Điểm trung bình ⭐", fontsize=11)
plt.ylabel("Tên sản phẩm", fontsize=11)
plt.title("Top 10 sản phẩm có điểm đánh giá cao nhất", fontsize=13, fontweight="bold")
plt.gca().invert_yaxis()

# Ghi nhãn điểm lên thanh
for bar in bars:
    plt.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
             f"{bar.get_width():.2f}", va="center", fontsize=9)

plt.tight_layout()
plt.show()

# ========== Biểu đồ 2 ==========
top_reviewed = merged_df.sort_values("review_count", ascending=False).head(10)
plt.figure(figsize=(10, 6))
bars = plt.barh(
    [shorten(x) for x in top_reviewed["title"]],
    top_reviewed["review_count"],
    color="skyblue"
)
plt.xlabel("Số lượng đánh giá 💬", fontsize=11)
plt.ylabel("Tên sản phẩm", fontsize=11)
plt.title("Top 10 sản phẩm có nhiều đánh giá nhất", fontsize=13, fontweight="bold")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# ========== Biểu đồ 3 ==========
plt.figure(figsize=(8, 6))
plt.scatter(merged_df["review_count"], merged_df["avg_rating"], alpha=0.6, color="#9b59b6")
plt.title("Tương quan giữa điểm trung bình và số lượng đánh giá", fontsize=13, fontweight="bold")
plt.xlabel("Số lượng đánh giá", fontsize=11)
plt.ylabel("Điểm trung bình ⭐", fontsize=11)
plt.grid(True)
plt.tight_layout()
plt.show()

# ========== Biểu đồ 4 ==========
top_sold = merged_df.sort_values("sold_num", ascending=False).head(10)
plt.figure(figsize=(10, 6))
bars = plt.barh(
    [shorten(x) for x in top_sold["title"]],
    top_sold["sold_num"],
    color="orange"
)
plt.xlabel("Số lượng đã bán", fontsize=11)
plt.ylabel("Tên sản phẩm", fontsize=11)
plt.title("Top 10 sản phẩm bán chạy nhất", fontsize=13, fontweight="bold")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# ==============================
# 4️⃣ Xuất dữ liệu xử lý ra file CSV
# ==============================
merged_df.to_csv("product_summary.csv", index=False, encoding="utf-8-sig")
print("📄 Đã lưu file 'product_summary.csv' để xem trong Excel hoặc Pandas.")
