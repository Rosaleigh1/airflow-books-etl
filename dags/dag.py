from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from urllib.parse import urljoin
from sqlalchemy import create_engine

# ✅ แก้ไข 1: ชื่อต้องตรงกับที่ตั้งใน Airflow UI (ตัวเล็กหมด)
DB_CONN_ID = "amazon_books_local_db"

# --- Function 1: Extract & Transform ---
def get_all_books(ti):
    base_site_url = "http://books.toscrape.com/index.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    all_books = []

    print("กำลังเริ่มค้นหาหมวดหมู่ทั้งหมด...")
    response = requests.get(base_site_url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    category_links = soup.select(".nav-list > li > ul > li > a")

    # เพื่อความรวดเร็วตอน Test ขอตัดเหลือแค่ 1 หมวดพอนะครับ (ถ้าจะเอาหมดให้ลบบรรทัดนี้)
    # category_links = category_links[:1]

    for link in category_links:
        category_name = link.text.strip()
        relative_url = link["href"]
        current_url = urljoin("http://books.toscrape.com/", relative_url)

        print(f"--> กำลังดึงข้อมูลหมวด: {category_name}")

        while True:
            try:
                cat_response = requests.get(current_url, headers=headers)
                cat_soup = BeautifulSoup(cat_response.content, "html.parser")
                book_containers = cat_soup.find_all("article", {"class": "product_pod"})

                for book in book_containers:
                    title_tag = book.find("h3").find("a")
                    book_title = title_tag["title"] if title_tag else "Unknown"

                    price_tag = book.find("p", {"class": "price_color"})
                    book_price = price_tag.text.strip() if price_tag else "0"

                    rating_tag = book.find("p", {"class": "star-rating"})
                    book_rating = "N/A"
                    if rating_tag:
                        classes = rating_tag.get("class", [])
                        if len(classes) >= 2:
                            book_rating = classes[1]

                    all_books.append(
                        {
                            "Title": book_title,
                            "Author": "Toscrape Author",
                            "Price": book_price,
                            "Rating": book_rating,
                            "Category": category_name,
                        }
                    )

                next_button = cat_soup.find("li", {"class": "next"})
                if next_button:
                    next_url = next_button.find("a")["href"]
                    current_url = urljoin(current_url, next_url)
                else:
                    break
            except Exception as e:
                print(f"Error: {e}")
                break

    print(f"สรุป: ดึงข้อมูลได้ทั้งหมด {len(all_books)} เล่ม")
    ti.xcom_push(key="book_data", value=all_books)


# --- Function 2: Load to Postgres ---
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key="book_data", task_ids="fetch_book_data")
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    insert_query = """
    INSERT INTO books (title, authors, price, rating, category)
    VALUES (%s, %s, %s, %s, %s)
    """

    for book in book_data:
        postgres_hook.run(
            insert_query,
            parameters=(
                book["Title"],
                book["Author"],
                book["Price"],
                book["Rating"],
                book["Category"],
            ),
        )


# --- Function 3: Export to Excel ---
def export_to_excel():
    print("กำลังเริ่มสร้างรายงาน Excel...")

    # ใช้ Hook ดึง URI เพื่อสร้าง Engine (แก้ปัญหา SQLAlchemy Warning)
    postgres_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    db_uri = postgres_hook.get_uri()
    engine = create_engine(db_uri)

    sql_query = "SELECT * FROM books;"
    df = pd.read_sql(sql_query, engine)

    file_path = "/opt/airflow/dags/books_report.xlsx"
    df.to_excel(file_path, index=False, engine="openpyxl")

    print(f"✅ สร้างไฟล์สำเร็จ! อยู่ที่: {file_path}")


# --- DAG Definition ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_and_store_amazon_books",
    default_args=default_args,
    description="Fetch all books with categories from BooksToScrape",
    schedule=timedelta(days=1),
)

# --- Tasks ---

fetch_book_data_task = PythonOperator(
    task_id="fetch_book_data",
    python_callable=get_all_books,
    dag=dag,
)

# ✅ แก้ไข 2: สร้างตารางชื่อ books และใส่ชื่อ Column ให้ครบ
create_table_task = SQLExecuteQueryOperator(
    task_id="create_table_dw",
    conn_id=DB_CONN_ID,
    sql="""
    DROP TABLE IF EXISTS books;
    
    CREATE TABLE books (
        id SERIAL PRIMARY KEY,
        title TEXT,
        authors TEXT,
        price TEXT,
        rating TEXT,
        category TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id="insert_book_data",
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

export_excel_task = PythonOperator(
    task_id="export_to_excel",
    python_callable=export_to_excel,
    dag=dag,
)

# --- Dependencies ---
fetch_book_data_task >> create_table_task >> insert_book_data_task >> export_excel_task

