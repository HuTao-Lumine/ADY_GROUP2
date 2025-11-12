import pandas as pd
import mysql.connector

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import sys 

# ==============================================================================
# # PHẦN 1: KẾT NỐI CƠ SỞ DỮ LIỆU MYSQL
# ==============================================================================
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "25251325" # Mật khẩu của bạn
DB_NAME = "review_crawl"

print("--- PHẦN 1: KẾT NỐI CƠ SỞ DỮ LIỆU ---")
try:
    from sqlalchemy import create_engine
    import urllib.parse

    # Mật khẩu được mã hóa an toàn cho chuỗi kết nối
    password = urllib.parse.quote_plus("123456789")
    # Tạo engine kết nối SQLAlchemy
    engine = create_engine(f"mysql+mysqlconnector://root:{password}@{DB_HOST}/{DB_NAME}")

    print("✅ Kết nối MySQL thành công! Biến 'engine' đã sẵn sàng.")
except Exception as e:
    print(f"❌ LỖI KẾT NỐI MySQL: {e}")
    sys.exit() 

# Cài đặt giao diện trực quan hóa
sns.set_style("whitegrid")


# ==============================================================================
# # PHẦN 2: TẢI DỮ LIỆU THÔ TỪ CƠ SỞ DỮ LIỆU
# ==============================================================================
print("\n--- PHẦN 2: TẢI DỮ LIỆU THÔ (Đợt 7 và Đợt 8) ---")

# --- TẢI DỮ LIỆU ĐỢT 7 (T7) ---
try:
    print(" -> 2.1: Đang tải dữ liệu Đợt 7 (products_dot_7 & reviews_dot_7)...")
    df_products_t7 = pd.read_sql('SELECT * FROM products_dot_7', con=engine)
    df_reviews_t7 = pd.read_sql('SELECT * FROM reviews_dot_7', con=engine)
    print(" ✅ Đợt 7: Tải thành công 2 bảng thô.")
except Exception as e:
    print(f"❌ LỖI KHI TẢI DỮ LIỆU ĐỢT 7: {e}")
    sys.exit() 

# --- TẢI DỮ LIỆU ĐỢT 8 (T8) ---
try:
    print("\n -> 2.2: Đang tải dữ liệu Đợt 8 (products_dot_8 & reviews_dot_8)...") 
    df_products_t8 = pd.read_sql('SELECT * FROM products_dot_8', con=engine)
    df_reviews_t8 = pd.read_sql('SELECT * FROM reviews_dot_8', con=engine)
    
    # In ra số lượng hàng ban đầu
    print(f" ℹ️ Kích thước ban đầu:")
    print(f"   -> df_products_t7: {len(df_products_t7)} hàng")
    print(f"   -> df_reviews_t7:  {len(df_reviews_t7)} hàng")
    print(f"   -> df_products_t8: {len(df_products_t8)} hàng")
    print(f"   -> df_reviews_t8:  {len(df_reviews_t8)} hàng")
except Exception as e:
    print(f"❌ LỖI KHI TẢI DỮ LIỆU ĐỢT 8: {e}")
    sys.exit()


# ==============================================================================
# # PHẦN 3: LỌC, XỬ LÝ VÀ ĐỒNG BỘ DỮ LIỆU GIỮA 2 ĐỢT (T7 và T8)
# ==============================================================================
print("\n--- PHẦN 3: XỬ LÝ VÀ ĐỒNG BỘ DỮ LIỆU ---")

# --- 3.1: Chuẩn hóa Keys (Link và product_id) ---
print(" -> 3.1: Đang dọn dẹp và chuẩn hóa định dạng Keys (Link và ID)...")
LINK_COLUMN_NAME = 'link' 

# Áp dụng cho T7
df_products_t7[LINK_COLUMN_NAME] = df_products_t7[LINK_COLUMN_NAME].astype(str).str.strip()
df_products_t7['product_id'] = pd.to_numeric(df_products_t7['product_id'], errors='coerce').fillna(0).astype(int).astype(str)
df_reviews_t7['product_id'] = pd.to_numeric(df_reviews_t7['product_id'], errors='coerce').fillna(0).astype(int).astype(str)

# Áp dụng cho T8
df_products_t8[LINK_COLUMN_NAME] = df_products_t8[LINK_COLUMN_NAME].astype(str).str.strip()
df_products_t8['product_id'] = pd.to_numeric(df_products_t8['product_id'], errors='coerce').fillna(0).astype(int).astype(str)
df_reviews_t8['product_id'] = pd.to_numeric(df_reviews_t8['product_id'], errors='coerce').fillna(0).astype(int).astype(str)
print(" ✅ Dọn dẹp Keys hoàn tất.")

# --- 3.2: Xóa trùng lặp Link (Chỉ giữ lại bản ghi đầu tiên) ---
print(" -> 3.2: Đang xóa các link trùng lặp trong bảng Products...")
df_products_t7.drop_duplicates(subset=[LINK_COLUMN_NAME], keep='first', inplace=True)
df_products_t8.drop_duplicates(subset=[LINK_COLUMN_NAME], keep='first', inplace=True)
print(f"   -> df_products_t7 sau khi lọc trùng: {len(df_products_t7)} hàng")
print(f"   -> df_products_t8 sau khi lọc trùng: {len(df_products_t8)} hàng")


# --- 3.3: Tìm và Lọc Danh sách Link chung (INNER JOIN logic) ---
print(" -> 3.3: Đang tìm và lọc các Sản phẩm tồn tại ở cả hai đợt (Link chung)...")
links_t7 = set(df_products_t7[LINK_COLUMN_NAME])
links_t8 = set(df_products_t8[LINK_COLUMN_NAME])
common_links = links_t7.intersection(links_t8)
print(f"   -> Tìm thấy {len(common_links)} link chung để phân tích.")

# Lọc các bảng Products
df_products_t7 = df_products_t7[df_products_t7[LINK_COLUMN_NAME].isin(common_links)].copy()
df_products_t8 = df_products_t8[df_products_t8[LINK_COLUMN_NAME].isin(common_links)].copy()
print(f"   -> df_products_t7 (đồng bộ): {len(df_products_t7)} hàng")
print(f"   -> df_products_t8 (đồng bộ): {len(df_products_t8)} hàng")

# --- 3.4: Lọc các bảng Reviews theo ID Sản phẩm đã được đồng bộ ---
print(" -> 3.4: Đang lọc các bảng Reviews theo Product ID đã đồng bộ...")
valid_ids_t7 = set(df_products_t7['product_id'])
valid_ids_t8 = set(df_products_t8['product_id'])

df_reviews_t7 = df_reviews_t7[df_reviews_t7['product_id'].isin(valid_ids_t7)].copy()
df_reviews_t8 = df_reviews_t8[df_reviews_t8['product_id'].isin(valid_ids_t8)].copy()

print(f"   -> df_reviews_t7 (sau lọc): {len(df_reviews_t7)} hàng")
print(f"   -> df_reviews_t8 (sau lọc): {len(df_reviews_t8)} hàng")

print("\n✅ PHẦN 3 HOÀN TẤT. Các bảng đã được đồng bộ để phân tích.")

# ==============================================================================
# # PHẦN 4: TẠO DATAFRAME TỔNG HỢP (df_analysis) VÀ TÍNH TOÁN CHÊNH LỆCH
# ==============================================================================
print("\n--- PHẦN 4: TẠO DATAFRAME TỔNG HỢP (df_analysis) ---")

# --- 4.1: Chuẩn bị bảng T7 (Làm bảng gốc) ---
print(" -> 4.1: Chuẩn bị bảng T7 (avg_rate_t7, review_count_7)...")
products_t7_simple = df_products_t7[
    ['link', 'product_id', 'title', 'avg_rate', 'review_count']
].copy()

products_t7_simple = products_t7_simple.rename(columns={
    'avg_rate': 'avg_rate_t7',
    'review_count': 'review_count_7'
})

# --- 4.2: Chuẩn bị bảng T8 ---
print(" -> 4.2: Chuẩn bị bảng T8 (review_count_8)...")
products_t8_simple = df_products_t8[
    ['link', 'review_count']
].copy()

products_t8_simple = products_t8_simple.rename(columns={
    'review_count': 'review_count_8'
})

# --- 4.3: Gộp (Merge) T7 và T8 dựa trên 'link' ---
print(" -> 4.3: Gộp (Merge) T7 và T8...")
df_analysis = pd.merge(products_t7_simple, products_t8_simple, on='link', how='inner')
print(f" ✅ Gộp bảng hoàn tất! 'df_analysis' có {len(df_analysis)} hàng.")

# --- 4.4: Tính toán chênh lệch số lượng Review (review_count_diff) ---
print(" -> 4.4: Đang tính 'review_count_diff' (T8 - T7)...")
df_analysis['review_count_diff'] = df_analysis['review_count_8'] - df_analysis['review_count_7']
print(" ✅ Đã tính xong độ chênh lệch review.")

# --- 4.5: Hiển thị kết quả cuối cùng ---
print("\n✅ PHẦN 4 HOÀN TẤT!")
print(" -> DataFrame 'df_analysis' đã sẵn sàng (5 dòng đầu):")

final_columns = [
    'product_id', 
    'title', 
    'link', 
    'avg_rate_t7', 
    'review_count_7', 
    'review_count_8', 
    'review_count_diff'
]
print(df_analysis[final_columns].head())


# ==============================================================================
# # PHẦN 5: TÍNH TOÁN MA TRẬN TƯƠNG QUAN
# ==============================================================================
print("\n--- PHẦN 5: TÍNH TOÁN MA TRẬN TƯƠNG QUAN ---")

# Chọn các cột số liên quan để tính tương quan
corr_df = df_analysis[[
    'avg_rate_t7', 
    'review_count_7', 
    'review_count_8', 
    'review_count_diff'
]]
correlation_matrix = corr_df.corr()

print(" ➡️ Ma trận tương quan (Correlation Matrix):")
print(correlation_matrix)


# ==============================================================================
# # PHẦN 6: TRỰC QUAN HÓA DỮ LIỆU
# ==============================================================================
print("\n--- PHẦN 6: TRỰC QUAN HÓA DỮ LIỆU ---")

# 6.1: Heatmap của Ma trận tương quan
print(" -> 6.1: Heatmap: Trực quan hóa Ma trận Tương quan (Hiển thị mối quan hệ giữa các biến)")
plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, 
            annot=True,     # Hiển thị giá trị số trên biểu đồ
            cmap='coolwarm',  # Chọn bảng màu 'coolwarm'
            fmt='.2f'       # Định dạng số thập phân 2 chữ số
           )
plt.title('Ma trận tương quan (Review Counts & Rating)')
plt.show() 
print(" ✅ Heatmap đã được hiển thị.")


# 6.2: Biểu đồ Tán xạ có Đường xu hướng (Regression Plot)
print(" -> 6.2: Regplot: Mối quan hệ giữa Rating (T7) và Tăng trưởng Review (diff)")
sns.regplot(data=df_analysis, 
            x='avg_rate_t7', 
            y='review_count_diff', 
            scatter_kws={'alpha':0.3} # Giảm độ đậm của các điểm
           )
plt.title('Tăng trưởng Review (diff) vs. Rating trung bình (T7)')
plt.xlabel('Rating Trung bình Đợt 7')
plt.ylabel('Tăng trưởng Review (Đợt 8 - Đợt 7)')
plt.show()

# 6.3: Biểu đồ Histogram (Phân bố)
print(" -> 6.3: Histograms: Xem phân bố của Tăng trưởng Review và Rating")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Histogram cho Tăng trưởng Review
sns.histplot(df_analysis['review_count_diff'], bins=50, kde=True, ax=ax1)
ax1.set_title('Phân bố Tăng trưởng Review (diff)')
ax1.set_xlabel('Tăng trưởng Review (Số lượng)')
ax1.set_ylabel('Tần suất')

# Histogram cho Rating trung bình
sns.histplot(df_analysis['avg_rate_t7'], bins=20, kde=True, ax=ax2)
ax2.set_title('Phân bố Rating trung bình (T7)')
ax2.set_xlabel('Rating Trung bình')
ax2.set_ylabel('Tần suất')

plt.show()

# 6.4: Biểu đồ Tán xạ so sánh Số lượng Review T7 vs T8
print(" -> 6.4: Scatterplot: So sánh Số lượng Review tuyệt đối (T7 vs T8)")
plt.figure(figsize=(8, 6))
sns.scatterplot(
    data=df_analysis,
    x='review_count_7',
    y='review_count_8',
    alpha=0.6
)
plt.title("So sánh số lượng review giữa đợt 7 và đợt 8")
plt.xlabel("Số review đợt 7")
plt.ylabel("Số review đợt 8")

# Thêm đường chéo y=x (Đường Tham Chiếu: Tăng/giảm bằng nhau)
max_val = max(df_analysis['review_count_7'].max(), df_analysis['review_count_8'].max()) * 1.05 # Thêm 5% cho khoảng trống
plt.plot([0, max_val], [0, max_val], color='red', linestyle='--', label='Đường tham chiếu (T7=T8)')
plt.legend()
plt.show()

# 6.5: Biểu đồ Hộp (Box Plot) cho Phân bố Tăng trưởng Review theo Nhóm Rating
print(" -> 6.5: Box Plot: Phân bố Tăng trưởng Review theo Mức Rating (Phân tích chi tiết)")

# Phân loại cột 'avg_rate_t7' thành các nhóm (Rating_Band)
bins = [0, 4.0, 4.3, 4.6, 5.0]
labels = ['Dưới 4.0', '4.0 - 4.2', '4.3 - 4.5', '4.6 - 5.0']
df_analysis['Rating_Band'] = pd.cut(df_analysis['avg_rate_t7'], 
                                    bins=bins, 
                                    labels=labels, 
                                    right=False # Khoảng [min, max)
                                   )

# Thiết lập thứ tự hiển thị của các nhóm trên trục X
order_labels = ['4.6 - 5.0', '4.3 - 4.5', '4.0 - 4.2', 'Dưới 4.0'] 

plt.figure(figsize=(12, 7))

# Tạo Box Plot
sns.boxplot(
    x='Rating_Band', 
    y='review_count_diff', 
    data=df_analysis, 
    order=order_labels,
    palette='viridis' 
)

plt.title('Phân bố Tăng trưởng Review (diff) theo Mức Rating Trung bình')
plt.xlabel('Mức Rating Trung bình (T7)')
plt.ylabel('Tăng trưởng Review (Số lượng)')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

print(" ✅ Box Plot đã được hiển thị.")