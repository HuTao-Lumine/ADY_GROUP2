import pandas as pd
import mysql.connector

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import sys 
#pahn1: ket noi mysql
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "@" #
DB_NAME = "review_crawl"


print("Đang kết nối tới MySQL...")
try:
    from sqlalchemy import create_engine
    import urllib.parse

    password = urllib.parse.quote_plus("@Vlmtritin2005")
    engine = create_engine(f"mysql+mysqlconnector://root:{password}@localhost/review_crawl")

    print("Kết nối thành công! Biến 'engine' đã sẵn sàng.")
except Exception as e:
    print(f"LỖI KẾT NỐI: {e}")
    sys.exit() 

sns.set_style("whitegrid")



# PHẦN 2: TẢI DỮ LIỆU T

print("BƯỚC 2: TẢI DỮ LIỆU THÔ (Cả 2 đợt)")

try:
    print(" -> Đang tải Đợt 1 (từ _dot_1)...")
    
    df_products_t7 = pd.read_sql('SELECT * FROM products_dot_7', con=engine)
    
    df_reviews_t7 = pd.read_sql('SELECT * FROM reviews_dot_7', con=engine)
    print("Đợt 1: Tải thành công 2 bảng thô.")

except Exception as e:
    print(f"LỖI KHI TẢI ĐỢT 1: {e}")
    sys.exit() 

try:
    print("\n -> Đang tải Đợt 2 (từ _dot_2)...") 

    df_products_t8 = pd.read_sql('SELECT * FROM products_dot_8', con=engine)
    
    df_reviews_t8 = pd.read_sql('SELECT * FROM reviews_dot_8', con=engine)
    
    print(f" -> df_products_t7: {len(df_products_t7)} hàng")
    print(f" -> df_reviews_t7:  {len(df_reviews_t7)} hàng")
    print(f" -> df_products_t8: {len(df_products_t8)} hàng")
    print(f" -> df_reviews_t8:  {len(df_reviews_t8)} hàng")

except Exception as e:
    print(f"LỖI KHI TẢI ĐỢT 2: {e}")
    sys.exit()


print("\nBƯỚC 3 (v2): LỌC (INNER JOIN) VÀ XÓA TRÙNG LẶP")

print(" -> 3.1: Đang dọn dẹp Keys (Link và ID)...")
LINK_COLUMN_NAME = 'link' 


df_products_t7[LINK_COLUMN_NAME] = df_products_t7[LINK_COLUMN_NAME].astype(str).str.strip()
df_products_t7['product_id'] = pd.to_numeric(df_products_t7['product_id'], errors='coerce').fillna(0).astype(int).astype(str)
df_reviews_t7['product_id'] = pd.to_numeric(df_reviews_t7['product_id'], errors='coerce').fillna(0).astype(int).astype(str)


df_products_t8[LINK_COLUMN_NAME] = df_products_t8[LINK_COLUMN_NAME].astype(str).str.strip()
df_products_t8['product_id'] = pd.to_numeric(df_products_t8['product_id'], errors='coerce').fillna(0).astype(int).astype(str)
df_reviews_t8['product_id'] = pd.to_numeric(df_reviews_t8['product_id'], errors='coerce').fillna(0).astype(int).astype(str)
print(" -> 3.1: Dọn dẹp Keys hoàn tất.")

# --- 3.2: Xóa trùng lặp Link (SỬA LỖI) ---
print(" -> 3.2: Đang xóa các link trùng lặp (chỉ giữ lại 1)...")
# Giữ lại 'link' đầu tiên nó thấy
df_products_t7.drop_duplicates(subset=[LINK_COLUMN_NAME], keep='first', inplace=True)
df_products_t8.drop_duplicates(subset=[LINK_COLUMN_NAME], keep='first', inplace=True)
print(f" -> df_products_t7 (sạch): {len(df_products_t7)} hàng")
print(f" -> df_products_t8 (sạch): {len(df_products_t8)} hàng")


# --- 3.3: Tìm danh sách Link chung ---
print(" -> 3.3: Đang tìm các link chung...")
links_t7 = set(df_products_t7[LINK_COLUMN_NAME])
links_t8 = set(df_products_t8[LINK_COLUMN_NAME])

common_links = links_t7.intersection(links_t8)
print(f" -> 3.3: Tìm thấy {len(common_links)} link chung ở cả hai đợt.")

# --- 3.4: Lọc các bảng Products (Bây giờ sẽ khớp) ---
print(" -> 3.4: Đang lọc các bảng Products...")
df_products_t7 = df_products_t7[df_products_t7[LINK_COLUMN_NAME].isin(common_links)].copy()
df_products_t8 = df_products_t8[df_products_t8[LINK_COLUMN_NAME].isin(common_links)].copy()

print(f" -> df_products_t7 (mới): {len(df_products_t7)} hàng")
print(f" -> df_products_t8 (mới): {len(df_products_t8)} hàng")

# --- 3.5: Lọc các bảng Reviews (Bây giờ sẽ hoạt động) ---
print(" -> 3.5: Đang lọc các bảng Reviews...")
valid_ids_t7 = set(df_products_t7['product_id'])
valid_ids_t8 = set(df_products_t8['product_id'])

df_reviews_t7 = df_reviews_t7[df_reviews_t7['product_id'].isin(valid_ids_t7)].copy()
df_reviews_t8 = df_reviews_t8[df_reviews_t8['product_id'].isin(valid_ids_t8)].copy()

print(f" -> df_reviews_t7 (mới): {len(df_reviews_t7)} hàng")
print(f" -> df_reviews_t8 (mới): {len(df_reviews_t8)} hàng")

print("\n. Các bảng đã được đồng bộ.")

# BƯỚC 4: TẠO DATAFRAME TỔNG HỢP (df_analysis)

#  gộp dựa trên 'link' 



#laycuabang7
products_t7_simple = df_products_t7[
    ['link', 'product_id', 'title', 'avg_rate', 'review_count']
].copy()

# Đổi tên cột để chuẩn bị gộp
products_t7_simple = products_t7_simple.rename(columns={
    'avg_rate': 'avg_rate_t7',
    'review_count': 'review_count_7'
})

# --- 4.2: Chuẩn bị bảng T8 ---
# Lấy các cột bạn cần từ T8 (chỉ cần 'link' và 'review_count')
products_t8_simple = df_products_t8[
    ['link', 'review_count']
].copy()

# Đổi tên cột
products_t8_simple = products_t8_simple.rename(columns={
    'review_count': 'review_count_8'
})

# Merge T7 và T8
df_analysis = pd.merge(products_t7_simple, products_t8_simple, on='link', how='inner')
print(f"     Gộp bảng hoàn tất! 'df_analysis' có {len(df_analysis)} hàng.")

#  Tính toán chênh lệch 
print(" -> 4.4: Đang tính 'review_count_diff'...")
df_analysis['review_count_diff'] = df_analysis['review_count_8'] - df_analysis['review_count_7']
print("     Đã tính xong độ chênh lệch review.")

# --- HOÀN TẤT ---
print("\nBƯỚC 4 HOÀN TẤT!")
print("     DataFrame 'df_analysis' đã sẵn sàng (5 dòng đầu):")

# Hiển thị các cột cuối cùng
final_columns = [
    'product_id',  # (Sẽ là ID từ Đợt 7)
    'title', 
    'link', 
    'avg_rate_t7', 
    'review_count_7', 
    'review_count_8', 
    'review_count_diff'
]
print(df_analysis[final_columns].head())

print(df_analysis.head())

import matplotlib.pyplot as plt
import seaborn as sns

print(" ->Tính toán ma trận tương quan ")

# Chọn các cột số liên quan từ df_analysis MỚI
corr_df = df_analysis[[
    'avg_rate_t7', 
    'review_count_7', 
    'review_count_8', 
    'review_count_diff'
]]
correlation_matrix = corr_df.corr()

print("Ma trận tương quan:")
print(correlation_matrix)
print("\n")

# --- PHẦN 6.2: Trực quan hóa (Heatmap) ---
print(" -> 6.2: Đang tạo heatmap và hiển thị...")

plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Ma trận tương quan (Review Counts & Rating)')


plt.show() 

print(" Heatmap đã được hiển thị.")
# Dùng regplot để vẽ đường xu hướng
sns.regplot(data=df_analysis, x='avg_rate_t7', y='review_count_diff', 
            scatter_kws={'alpha':0.3})
plt.title('TReview diff vs. Rating trung bình (T7)')
plt.show()
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
sns.histplot(df_analysis['review_count_diff'], bins=50, kde=True, ax=ax1)
ax1.set_title('Phân bố Tăng trưởng Review (diff)')
sns.histplot(df_analysis['avg_rate_t7'], bins=20, kde=True, ax=ax2)
ax2.set_title('Phân bố Rating trung bình (T7)')
plt.show()