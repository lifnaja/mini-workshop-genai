
### Prompt 1
ช่วย setup airflow dags ให้หน่อย โดยจะมี task โดยให้มี task ประมาณนี้
task ที่ 1 scrape_cnbc_news_task ทำหน้าที่ scrape ข่าวจาก CNBC
task ที่ 2 save_raw_to_gcs_task ทำหน้าที่ save ข่าวที่ scrape ลง GCS
task ที่ 3 summarize_with_gemini_task ให้ Gemini สรุปข่าว
task ที่ 4 save_summary_to_gcs_task save  ข่าวที่สรุปมาจาก AI ลง GCS

โดยอยากจะให้เริ่มจากทำเป็นโครงโดยใช้  EmptyOperator  ก่อนที่จะเขียน  ฟังก็ชั่นจริงๆ
ให้สร้าง dags ในโฟลเดอร์ workshop/dags/

### Prompt 2
ต่อไป ช่วยเขียนtask scrape_cnbc_news_task โดยใช้ PythonOperator โดยจะ scrape ข้อมูลจาก https://www.cnbc.com/world/?region=world
โดยข้อมูลที่จะ scrape จะมี url, title และเมื่อได้ url มาแล้วให้  scrape  content มาด้วย ให้เอาข่าวที่อยู่ ใน sector Latest News

ตัวอย่าง element ในส่วนของ Latest News
<li class="LatestNews-item" id="HomePageInternational-latestNews-7-1"><div class="LatestNews-container"><div class="LatestNews-headlineWrapper"><span class="LatestNews-wrapper"><time class="LatestNews-timestamp">2 Hours Ago</time></span><a href="https://www.cnbc.com/2025/07/07/laopu-gold-cartier-lvmh-richemont-chow-tai-fook-marina-bay-sands-china-singapore.html" class="LatestNews-headline" title="From Beijing to Marina Bay Sands: Laopu’s golden gamble pays off">From Beijing to Marina Bay Sands: Laopu’s golden gamble pays off</a></div></div></li>


### Prompt 3
ต่อไป ช่วยเขียน task save_raw_to_gcs_task โดยใช้ PythonOperator โดย save ข้อมูลจาก task scrape_cnbc_news_task เป็นไฟล์ csv ที่ GCS โดยใช้ GCSHook

โดย save ที่ที่ path news/cnbc/raw_news/{ds}/{ds}_news.csv


### Prompt 4
ต่อไป ช่วยเขียน task summarize_with_gemini_task โดยใช้ PythonOperator โดยโหลดข้อมูลจาก GCS แล้วให้ gemini summarize ข่าวทั้งหมดเป็นบทความเดียวแบบรวมทุกข่าว โดยใช้ xcom ดึง path s3 จาก task save_raw_to_gcs_task

โดยตัวอย่างการเรียกใช้ gemini api ให้ดูที่ไฟล์ example_gemini_api.py

และ gemini_api_key ให้ดึงมากจาก Airflow variable

### Prompt 5
ต่อไป ช่วยเขียน task save_summary_to_gcs_task โดยใช้ PythonOperator โดยเอา sumary จาก task summarize_with_gemini_task ไป save เป็นไฟล์แล้วเก็บไว้่ที่ GCS โดยใช้ GCSHook

โดย save ที่ที่ path news/cnbc/summarize/{ds}/{ds}_news.txt