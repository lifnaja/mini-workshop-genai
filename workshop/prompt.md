
### Prompt 1
ช่วย setup airflow dags ให้หน่อย โดยจะมี task โดยให้มี task ประมาณนี้
task ที่ 1 scrape_cnbc_news_task ทำหน้าที่ scrape ข่าวจาก CNBC
task ที่ 2 save_raw_to_gcs_task ทำหน้าที่ save ข่าวที่ scrape ลง GCS
task ที่ 3 summarize_with_gemini_task ให้ Gemini สรุปข่าว
task ที่ 4 save_summary_to_gcs_task save  ข่าวที่สรุปมาจาก AI ลง GCS

โดยอยากจะให้เริ่มจากทำเป็นโครงโดยใช้  EmptyOperator  ก่อนที่จะเขียน  ฟังก็ชั่นจริงๆ

### Prompt 2
ต่อไป ช่วยเขียนtask scrape_cnbc_news_task โดยใช้ PythonOperator โดยจะ scrape ข้อมูลจาก https://www.cnbc.com/world/?region=world
โดยข้อมูลที่จะ scrape จะมี url, title และเมื่อได้ url มาแล้วให้  scrape  content มาด้วย ให้เอาข่าวที่อยู่ ใน sector Latest News


### Prompt 3
ต่อไป ช่วยเขียน task save_raw_to_gcs_task โดยใช้ PythonOperator โดย save ข้อมูลจาก task scrape_cnbc_news_task เป็นไฟล์ csv ที่ GCS โดยใช้ GCSHook


### Prompt 4
ต่อไป ช่วยเขียน task summarize_with_gemini_task โดยใช้ PythonOperator โดยโหลดข้อมูลจาก GCS แล้วให้ gemini summarize ข่าวทั้งหมดเป็นบทความเดียวแบบรวมทุกข่าว โดยใช้ xcom ดึง path s3 จาก task save_raw_to_gcs_task

### Prompt 5
ต่อไป ช่วยเขียน task save_summary_to_gcs_task โดยใช้ PythonOperator โดยเอา sumary จาก task summarize_with_gemini_task ไป save เป็นไฟล์แล้วเก็บไว้่ที่ GCS โดยใช้ GCSHook