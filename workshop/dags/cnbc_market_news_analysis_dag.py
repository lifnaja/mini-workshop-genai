from __future__ import annotations
import csv
import tempfile

import pendulum
import logging
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai
import pandas as pd


GCS_CONNECTION = "gcs_conn"
GCS_BUCKET_NAME = "BUCKET_NAME"
GEMINI_API_KEY = "GEMINI_API_KEY"

def scrape_cnbc_news():
    base_url = "https://www.cnbc.com"
    url = f"{base_url}/world/?region=world"

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch CNBC page: {response.status_code}")
    
    soup = BeautifulSoup(response.text, "html.parser")

    latest_news_list = soup.find("ul", class_="LatestNews-list")
    if not latest_news_list:
        raise ValueError("Latest News list not found")
    articles = latest_news_list.find_all("li", recursive=False)


    scraped_news = []
    for li in articles:
        link_tag = li.find("a", class_="LatestNews-headline", href=True)
        if not link_tag:
            continue
        
        title = link_tag.get_text(strip=True)
        news_url = link_tag["href"]

        if news_url.startswith("/"):
            news_url = f"{base_url}{news_url}"

        try:
            news_resp = requests.get(news_url, timeout=10, headers=headers)
            news_soup = BeautifulSoup(news_resp.text, "html.parser")
            article_div = news_soup.find("div", class_="ArticleBody-articleBody")
            paragraphs = article_div.find_all("p")

            content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        except Exception as e:
            content = ""
            logging.error(f"[Error fetching content: {e}]")

        scraped_news.append({
            "title": title,
            "url": news_url,
            "content": content,
            "scraped_at": datetime.now()
        })

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmpfile:
            writer = csv.DictWriter(tmpfile, fieldnames=["title", "url", "content", "scraped_at"])
            writer.writeheader()
            for item in scraped_news:
                writer.writerow(item)
            tmpfile_path = tmpfile.name

    return tmpfile_path

def save_scraped_news_to_gcs(**context):
    ti = context["ti"]
    ds = context["ds"]
    scraped_news_file = ti.xcom_pull(task_ids="scrape_cnbc_news_task")
    
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONNECTION)
    filename = f"{ds}_cnbc_news.csv"
    object_path = f"news/cnbc/raw_news/{ds}/{filename}"
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=object_path,
        filename=scraped_news_file,
        mime_type="text/csv"
    )

    return object_path

def summarize_news_with_gemini(**context):
    ti = context["ti"]
    ds = context["ds"]
    object_path = ti.xcom_pull(task_ids="save_raw_to_gcs_task")

    gcs_hook = GCSHook(gcp_conn_id=GCS_CONNECTION)
    local_path = "/tmp/temp_news.csv"
    gcs_hook.download(
        bucket_name=GCS_BUCKET_NAME,
        object_name=object_path,
        filename=local_path,
    )

    df = pd.read_csv(local_path)
    combined_text = ""
    for _, row in df.iterrows():
        combined_text += f"หัวข้อ: {row['title']}\nเนื้อหา: {row['content']}\n\n"

    prompt = (
        "ต่อไปนี้คือข่าวเศรษฐกิจจาก CNBC หลายหัวข้อ "
        "ช่วยสรุปให้เป็นบทความเดียวที่เข้าใจง่าย สำหรับนักลงทุน:\n\n" + combined_text
    )

    client = genai.Client(
        api_key=GEMINI_API_KEY,
    )
    model = "gemini-2.5-flash-preview-05-20"
    response = client.models.generate_content(
        model=model, contents=[prompt]
    )
    summary_news = response.text

    logging.info(summary_news)

    summary_news_file = f"/tmp/{ds}_summary_news.txt"
    with open(summary_news_file, "w", encoding="utf-8") as f:
        f.write(summary_news)

    return summary_news_file


def save_summary_to_gcs(**context):
    ti = context["ti"]
    ds = context["ds"]
    summary_news_file = ti.xcom_pull(task_ids="summarize_with_gemini_task")

    bucket_name = GCS_BUCKET_NAME
    object_path = f"news/cnbc/summary/{ds}/summary_{ds}.txt"

    gcs_hook = GCSHook(gcp_conn_id=GCS_CONNECTION)
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_path,
        filename=summary_news_file, 
        mime_type="text/plain; charset=utf-8",
    )

    print(f"Saved summary to: gs://{bucket_name}/{object_path}")
    return f"gs://{bucket_name}/{object_path}"

default_args = {
    'owner': 'investment_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='financial_market_analyst_daily_report',
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=['financial_analysis', 'market_news', 'ai_summary'],
    default_args=default_args,
    doc_md="""
    # Financial Market Analyst Daily Report DAG
    DAG นี้มีหน้าที่ในการ automate กระบวนการเตรียมรายงานข่าวและบทวิเคราะห์สำหรับนักวิเคราะห์การลงทุนและเศรษฐกิจ 
    โดยจะเริ่มตั้งแต่การ scrape ข่าวจาก CNBC สรุปด้วย AI (Gemini) และจัดเก็บผลลัพธ์ไว้ใน GCS เพื่อให้ทีมอื่นนำไปใช้งานต่อ

    **เวลาทำงาน:**
    * DAG จะเริ่มทำงานทุกวันเวลา 04:00 น. (ตามเวลาที่กำหนดใน start_date และ schedule_interval)
    * เป้าหมายคือการสรุปข้อมูลให้พร้อมก่อน 08:00 น.

    **Flow of tasks:**
    1. **Scrape CNBC News:** ดึงข้อมูลข่าวสารจากหน้าตลาดของ CNBC.
    2. **Save Raw to GCS:** บันทึกข้อมูลข่าวสารดิบที่ได้จากการ scrape ลง Google Cloud Storage.
    3. **Load and Prepare for AI:** โหลดข้อมูลจาก GCS มาเตรียมความพร้อมสำหรับการนำเข้า AI (เช่น การจัดรูปแบบ, การรวมข้อความ).
    4. **Summarize with Gemini:** ใช้โมเดล Gemini ในการสรุปเนื้อหาข่าว.
    5. **Save Summary to GCS:** บันทึกผลสรุปที่ได้จาก AI ลง Google Cloud Storage.
    """,
) as dag:
    scrape_cnbc_news_task = PythonOperator(
        task_id='scrape_cnbc_news_task',
        python_callable=scrape_cnbc_news,
        doc_md="""
        ### Scrape CNBC News
        Task นี้มีหน้าที่ในการดึงข้อมูลข่าวสารจากเว็บไซต์ CNBC โดยเฉพาะหน้าตลาด (https://www.cnbc.com/latest/)
        ในอนาคต: จะถูกแทนที่ด้วย `PythonOperator` ที่เรียกใช้ฟังก์ชัน Python เพื่อทำ Web Scraping (เช่น `requests`, `BeautifulSoup`).
        ผลลัพธ์: Raw HTML/Text content ของข่าวที่เกี่ยวข้อง.
        """,
    )

    save_raw_to_gcs_task = PythonOperator(
        task_id='save_raw_to_gcs_task',
        python_callable=save_scraped_news_to_gcs,
        doc_md="""
        ### Save Raw Data to GCS
        Task นี้มีหน้าที่ในการบันทึกข้อมูลข่าวสารดิบที่ได้จาก `scrape_cnbc_news_task` ลง Google Cloud Storage.
        ในอนาคต: จะถูกแทนที่ด้วย `PythonOperator` ที่เรียกใช้ฟังก์ชัน Python เพื่อบันทึกไฟล์ (เช่น `google.cloud.storage`).
        ผลลัพธ์: ไฟล์ JSON หรือ TXT ที่มีข้อมูลข่าวสารดิบเก็บอยู่ใน GCS.
        """,
    )

    summarize_with_gemini_task = PythonOperator(
        task_id='summarize_with_gemini_task',
        python_callable=summarize_news_with_gemini,
        doc_md="""
        ### Summarize with Gemini AI
        Task นี้มีหน้าที่ในการส่งข้อความ
        ไปยัง Google Gemini API เพื่อทำการสรุปเนื้อหาข่าว.
        ในอนาคต: จะถูกแทนที่ด้วย `PythonOperator` ที่เรียกใช้ฟังก์ชัน Python เพื่อเรียกใช้ Gemini API.
        ผลลัพธ์: ข้อความสรุปที่ได้จาก Gemini.
        """,
    )

    save_summary_to_gcs_task = PythonOperator(
        task_id='save_summary_to_gcs_task',
        python_callable=save_summary_to_gcs,
        doc_md="""
        ### Save AI Summary to GCS
        Task นี้มีหน้าที่ในการบันทึกผลสรุปที่ได้จาก Gemini AI (`summarize_with_gemini_task`) 
        ลง Google Cloud Storage เพื่อให้ทีมอื่นนำไปใช้งานต่อ (เช่น การจัดทำ infographic หรือบทความ).
        ในอนาคต: จะถูกแทนที่ด้วย `PythonOperator` ที่เรียกใช้ฟังก์ชัน Python เพื่อบันทึกไฟล์.
        ผลลัพธ์: ไฟล์ TXT หรือ JSON ที่มีข้อความสรุปเก็บอยู่ใน GCS.
        """,
    )


    (
        scrape_cnbc_news_task
        >> save_raw_to_gcs_task
        >> summarize_with_gemini_task
        >> save_summary_to_gcs_task
    )

