import time
import nodriver as uc
import os
import json
import re
import logging
import mysql.connector
from pathlib import Path
import pandas as pd

# thêm thư viện regression
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

DEBUG = True

async def main():
    first_run = False
    user_data_dir = os.path.join(os.getcwd(), "profile")
    if not os.path.exists(user_data_dir):
        os.makedirs(user_data_dir)
        first_run = True

    driver = await uc.start(
        user_data_dir=user_data_dir,
        browser_executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    )

    tab = await driver.get("https://shopee.vn/Gi%C3%A0y-D%C3%A9p-Nam-cat.11035801")
    if first_run:
        print("Please Login to Shopee in the opened browser window.")
        print("Press Enter after you have logged in...")
        input()
    await tab

    log.info('Getting products...')
    log.debug('wait for products to load...')

    while(True):
        e = await tab.select(".shopee_ic", timeout=60)
        tmp = await e.query_selector('a')
        if (tmp and tmp.attrs.href):
            break
        else:
            await tab.scroll_down(1000)
            time.sleep(1)
            
    [await tab.scroll_down(1000) for _ in range(3)]
    time.sleep(1)
        
    log.debug("get all products...")
    links = await tab.evaluate("""
(() => {
    let ret = []
    document.querySelectorAll('.shopee_ic').forEach((e)=>{
        let b = e.querySelector('a')
        if (b)
            ret.push(e.querySelector('a').href)
    })
    return ret
})()""")
    links = parse_string_arr(links)
    log.info(f"Found {len(links)} link")

    for i, link in enumerate(links, 1):
        log.info(f'Processing {i}/{len(links)}')
        log.debug(f'open link: {link}')
        try:
            product_info = await get_product_info(tab, link)
        except Exception as e:
            log.exception(f'exception when getting product info: {e}')
            time.sleep(1)
            continue
        if product_info is None:
            time.sleep(1)
            continue
        try:
            write_object_to_json(product_info)
            log.info(f'Done product: {product_info["title"]}')
        except Exception as e:
            log.exception(f'exception when writing json: {e}')
        finally:
            time.sleep(1)

    # sau khi crawl xong, chạy mô hình regression
    run_regression_model()
    driver.stop()


async def get_product_info(tab: uc.Tab, link):
    await tab.get(link)
    await tab
    
    a = await tab.select('.page-product', 60)
    time.sleep(1)
    [await tab.scroll_down(1000) for _ in range(3)]
    time.sleep(2)

    title = await a.query_selector('h1')
    title = title.text_all
    
    if skip_product(title):
        log.info(f'Skipping product: {title} -> {get_file_name(title)}.json already exists')
        return None
    
    no_sold = await a.query_selector('section:nth-child(2)>section:nth-child(2)>div>div:nth-child(2)>div')
    no_sold = no_sold.text_all
    cmt_list = []

    while True:
        tmp_cmt_list = await tab.evaluate("""
(() => {
    let ret = []
    let cmt_list = document.querySelectorAll('.shopee-product-comment-list>div')
    cmt_list.forEach(cmt => {
        let main_content = cmt.querySelector('div:nth-child(2)')
        let cmt_text_container = main_content.children[1]
        let cmt_text = cmt_text_container.innerText.trim()
        let username_and_rating = main_content.children[0]
        let username = username_and_rating.firstChild.innerText.trim()
        let rating = username_and_rating.children[1].querySelectorAll('svg.icon-rating-solid').length
        let meta = username_and_rating.children[2].innerText.trim()
        ret.push({
            'username': username,
            'metadata': meta,
            'rating': rating,
            'comment': cmt_text
        })
    })
    return ret
})()
""")
        tmp_cmt_list = unpack_to_dict(tmp_cmt_list)
        cmt_list.extend(tmp_cmt_list)

        has_next = await tab.evaluate("""
(() => {
    let next_btn = document.querySelector('.product-ratings__page-controller button.shopee-button-solid+button')
    if (!next_btn) return false
    if (!isNaN(parseInt(next_btn.innerText))) {
        next_btn.click(); return true
    }
    return false
})()
""")
        if not has_next:
            break
        time.sleep(1)

    return {
        'title':  title,
        'link': link,
        'sold': no_sold,
        'comments': cmt_list
    }


def skip_product(title) -> bool:
    file_path = os.path.join('products', f'{get_file_name(title)}')
    return os.path.exists(file_path)
    
def parse_string_arr(inp):
    return [i['value'] for i in inp if 'value' in i]

def unpack_to_dict(data):
    result = []
    for item in data:
        if item['type'] == 'object':
            obj_dict = {}
            for key, value_info in item['value']:
                obj_dict[key] = value_info['value']
            result.append(obj_dict)
    return result

def get_file_name(s):
    clean_title = re.sub(r'[<>:"/\\|?*]', '_', s)
    clean_title = re.sub(r'\s+', '_', clean_title.strip())
    return  f"{clean_title}.json"

def write_object_to_json(obj, output_dir="products"):
    os.makedirs(output_dir, exist_ok=True)
    if 'title' not in obj:
        log.warning(f"Object missing 'title' field, skipping: {obj}")
        return
    
    filename = get_file_name(obj['title'])
    filepath = os.path.join(output_dir, filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(obj, f, indent=4, ensure_ascii=False)
        log.debug(f"Successfully wrote: {filepath}")
    except Exception as e:
        log.debug(f"Error writing {filepath}: {e}")
        
    #connect Mysql 
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="25251325",  
        database="review_crawl"
    )
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO products (title, link, sold)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE sold=VALUES(sold)
    """, (obj['title'], obj['link'], obj['sold']))

    product_id = cursor.lastrowid
    if not product_id:
        cursor.execute("SELECT id FROM products WHERE link=%s", (obj['link'],))
        product_id = cursor.fetchone()[0]

    reviews_data = [(product_id, c['username'], c['metadata'], c['rating'], c['comment'])
                    for c in obj.get('comments', [])]
    if reviews_data:
        cursor.executemany("""
            INSERT INTO reviews (product_id, username, metadata, rating, comment)
            VALUES (%s, %s, %s, %s, %s)
        """, reviews_data)

    conn.commit()
    cursor.close()
    conn.close()


#  thêm phần Regression Modeling
def run_regression_model():
    log.info("Running regression model...")
    product_files = [os.path.join("products", f) for f in os.listdir("products") if f.endswith(".json")]
    data = []

    for f in product_files:
        with open(f, "r", encoding="utf-8") as file:
            obj = json.load(file)
            sold = re.sub(r'[^\d]', '', obj['sold'])  # lọc số
            sold = int(sold) if sold else 0
            comments = obj.get('comments', [])
            if len(comments) == 0: continue

            avg_rating = sum(c['rating'] for c in comments)/len(comments)
            num_reviews = len(comments)
            pos_ratio = sum(c['rating']>=4 for c in comments)/len(comments)
            neg_ratio = sum(c['rating']<=2 for c in comments)/len(comments)
            data.append([avg_rating, num_reviews, pos_ratio, neg_ratio, sold])

    df = pd.DataFrame(data, columns=["avg_rating", "num_reviews", "pos_ratio", "neg_ratio", "sold"])
    X = df[["avg_rating", "num_reviews", "pos_ratio", "neg_ratio"]]
    y = df["sold"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LinearRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    log.info(f"Regression Results → MSE: {mean_squared_error(y_test, y_pred):.2f}, R²: {r2_score(y_test, y_pred):.2f}")
    df["predicted_sold"] = model.predict(X)
    trending = df.sort_values("predicted_sold", ascending=False).head(10)
    log.info("Top 10 Trending Products:\n" + trending.to_string(index=False))


# ---------------------- Logging config ---------------------- #
log_path = os.path.join('log','main.log')
Path(os.path.split(log_path)[0]).mkdir(parents=True, exist_ok=True)
log: logging.Logger = logging.getLogger('Main')
log.setLevel(logging.DEBUG)
stream_log_handler = logging.StreamHandler()
stream_log_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
file_log_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
log_format = logging.Formatter(
    '[%(asctime)s][%(levelname)s]: %(message)s', datefmt='%d-%m-%Y][%H:%M:%S')
stream_log_handler.setFormatter(log_format)
file_log_handler.setFormatter(log_format)
log.addHandler(stream_log_handler)
log.addHandler(file_log_handler)

if __name__ == '__main__':
    log.info('Starting main...')
    uc.loop().run_until_complete(main())
