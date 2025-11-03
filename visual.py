import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import random
import numpy as np

# --- 1️⃣ Kết nối MySQL ---
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="@Vlmtritin2005",
    database="review_crawl"
)

# --- 2️⃣ Lấy dữ liệu ---
query = """
SELECT product_id, comment, rating, sentiment_score
FROM reviews
WHERE rating IS NOT NULL AND sentiment_score IS NOT NULL
"""
df = pd.read_sql(query, db)
db.close()

print(f"✅ Đã tải {len(df)} review có rating và sentiment_score.")

# --- 3️⃣ Gán trọng số ---
df['weight'] = np.where(df['comment'].isnull() | (df['comment'].str.strip() == ''), 0.7, 1.0)

# --- 4️⃣ Chọn sản phẩm ---
product_id = random.choice(df['product_id'].unique())
print(f"🎯 Hiển thị visual cho product_id = {product_id}")

df_product = df[df['product_id'] == product_id]

# --- 5️⃣ Histogram: phân bố điểm cảm xúc (theo trọng số) ---
plt.figure(figsize=(6,4))
plt.hist(df_product['sentiment_score'], bins=5, weights=df_product['weight'], edgecolor='black')
plt.title(f"Phân bố điểm cảm xúc (có trọng số) – Sản phẩm {product_id}")
plt.xlabel("Sentiment Score (1–5)")
plt.ylabel("Trọng số tổng hợp review")
plt.show()

# --- 6️⃣ Heatmap: tương quan rating và sentiment (theo trọng số) ---
plt.figure(figsize=(6,5))
pivot = df_product.pivot_table(values='weight', index='rating', columns='sentiment_score', aggfunc='sum', fill_value=0)
sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlGnBu")
plt.title(f"Tương quan Rating–Sentiment (theo trọng số) – Sản phẩm {product_id}")
plt.show()

# --- 7️⃣ Boxplot: so sánh rating và sentiment ---
plt.figure(figsize=(6,4))
sns.boxplot(data=df_product, x='rating', y='sentiment_score', width=0.6)
plt.title(f"So sánh Rating và Sentiment – Sản phẩm {product_id}")
plt.show()
