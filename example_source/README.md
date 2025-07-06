# Example Use case  : Source

## Understand Schema

**Prompt**
```
ช่วยสรุป schema ของ ไฟล์ json ให้หน่อย พร้อมอธิบาย field ต่างๆ ด้วย
```
พร้อมกับ upload file `sample_data_understand_schema.json`

ตัวอย่าง result จะอยู่ที่ `sample_project_dbt/models/source.yml`

## Generate Mock Data
**Prompt**
```
อยากจะ mock ข้อมูลการซื้อขายหุ้นไทย เพื่อไปทำ ระบบ recommendation ก่อนจะใช้ data จริง

ตัวอย่าง schema
type_of_order STRING
order_no STRING
account_no STRING
sec_symbol STRING
trading_datetime DATETIME
trading_unit NUMERIC
trading_price NUMERIC
trading_amt NUMERIC

เงื่อนไข
- type_of_order มีแค่ BUY or SELL
- อยากได้ account แค่ 200 account
- ข้อมูลการซื้อขาย 30 วัน
- trading_unit ต้องซื้อด้วยจำที่หารด้วย 100 ลงตัว
- ถ้าไม่มีหุ้นใน port ไม่สามารถขายได้
- แต่ละ account ไม่จำเป็นต้องซื้อขายทุกวัน
```

ในตัวอย่างจะให้ https://gemini.google.com/ ช่วย generate mock data ให้

ไฟล์ `generate_mock_data.py` จะเป็นตัวอย่าง Code ที่ gemini generate มาให้
และไฟล์ `mock_thai_stock_trades.csv` จะเป็นตัวอย่างข้อมูลที่สร้างมาจาก Code