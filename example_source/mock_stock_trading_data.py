import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --- กำหนดค่าเริ่มต้นและพารามิเตอร์ต่างๆ ---
num_accounts = 200  # จำนวนบัญชีที่ต้องการ
num_days = 30       # จำนวนวันของข้อมูลการซื้อขาย
# กำหนดช่วงวันที่ของข้อมูล โดยเริ่มจาก 30 วันที่แล้วจนถึงปัจจุบัน
start_date = datetime.now() - timedelta(days=num_days)
end_date = datetime.now()

# กำหนดหุ้นจำลอง (สามารถเพิ่มหุ้นไทยที่เป็นที่รู้จักได้ เช่น ADVANC, PTTEP, SCB, AOT, CPALL)
sec_symbols = ['AOT', 'ADVANC', 'PTTEP', 'SCB', 'CPALL', 'BDMS', 'GULF', 'KBANK', 'SCC', 'TRUE']

# กำหนดช่วงราคาหุ้นสำหรับแต่ละหุ้น (อิงจากราคาหุ้นจริงในตลาด SET เพื่อความสมจริง)
# ราคาจะถูกสุ่มในช่วงนี้
price_ranges = {
    'AOT': (50.0, 80.0),      # ท่าอากาศยานไทย
    'ADVANC': (180.0, 250.0), # แอดวานซ์ อินโฟร์ เซอร์วิส
    'PTTEP': (120.0, 160.0),  # ปตท. สำรวจและผลิตปิโตรเลียม
    'SCB': (90.0, 120.0),     # ธนาคารไทยพาณิชย์
    'CPALL': (50.0, 70.0),    # ซีพี ออลล์
    'BDMS': (20.0, 30.0),     # กรุงเทพดุสิตเวชการ
    'GULF': (35.0, 50.0),     # กัลฟ์ เอ็นเนอร์จี ดีเวลลอปเมนท์
    'KBANK': (120.0, 150.0),  # ธนาคารกสิกรไทย
    'SCC': (250.0, 350.0),    # ปูนซิเมนต์ไทย
    'TRUE': (5.0, 8.0),       # ทรู คอร์ปอเรชั่น
}

# สร้าง account_no (รหัสบัญชี)
account_numbers = [f'ACC{i:03d}' for i in range(1, num_accounts + 1)]

# Dictionary สำหรับเก็บ portfolio ของแต่ละ account
# key: account_no, value: dictionary ของหุ้นที่ถือครอง (key: sec_symbol, value: trading_unit)
account_portfolios = {acc: {} for acc in account_numbers}

# List สำหรับเก็บข้อมูลการซื้อขายทั้งหมด
all_transactions = []
order_counter = 1 # ตัวนับสำหรับสร้าง order_no ที่ไม่ซ้ำกัน

# --- เริ่มต้นสร้างข้อมูลการซื้อขายในแต่ละวัน ---
for day in range(num_days):
    current_date = start_date + timedelta(days=day)
    
    # สุ่มจำนวนการซื้อขายในแต่ละวัน เพื่อให้แต่ละวันมีจำนวนรายการไม่เท่ากัน
    # กำหนดช่วงที่เหมาะสมเพื่อให้มีข้อมูลมากพอ
    daily_transactions_count = random.randint(50, 300) 

    for _ in range(daily_transactions_count):
        # สุ่มเลือก account_no และ sec_symbol สำหรับรายการนี้
        account_no = random.choice(account_numbers)
        sec_symbol = random.choice(sec_symbols)
        
        # สุ่มเวลาในแต่ละวัน ให้อยู่ในช่วงเวลาทำการ (9:00 - 16:00 น.)
        trading_datetime = current_date + timedelta(
            hours=random.randint(9, 16),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # กำหนดช่วงราคาสำหรับหุ้นนั้นๆ จาก price_ranges ที่กำหนดไว้
        min_price, max_price = price_ranges[sec_symbol]
        # สุ่มราคาซื้อขาย และปัดเศษให้เหลือ 2 ตำแหน่งทศนิยม
        trading_price = round(random.uniform(min_price, max_price), 2)
        
        # สุ่มจำนวนหุ้นที่หารด้วย 100 ลงตัว (ตามเงื่อนไข)
        # np.arange(start, stop, step) สร้าง array ของตัวเลข
        trading_unit = random.choice(np.arange(100, 10000, 100)) 
        
        # สุ่มประเภทคำสั่งซื้อ/ขาย
        type_of_order = random.choice(['BUY', 'SELL'])

        # --- ตรวจสอบเงื่อนไข "ถ้าไม่มีหุ้นใน port ไม่สามารถขายได้" ---
        if type_of_order == 'SELL':
            # ตรวจสอบว่า account มีหุ้น sec_symbol นี้อยู่หรือไม่ และมีจำนวนเพียงพอที่จะขายหรือไม่
            if account_portfolios[account_no].get(sec_symbol, 0) >= trading_unit:
                # ถ้ามีหุ้นเพียงพอ ให้ทำการขาย
                account_portfolios[account_no][sec_symbol] -= trading_unit
            else:
                # ถ้าไม่มีหุ้นเพียงพอที่จะขาย ให้เปลี่ยนเป็นคำสั่งซื้อแทน
                # เพื่อให้เกิดรายการซื้อขายขึ้นเสมอ
                type_of_order = 'BUY'
                # ถ้าต้องการให้รายการนี้ถูกข้ามไปเลย สามารถใช้ 'continue' ได้
                # continue 
        
        # --- ทำการซื้อ (ถ้าเป็นคำสั่งซื้อ หรือถูกเปลี่ยนจากขายมาเป็นซื้อ) ---
        if type_of_order == 'BUY':
            # เพิ่มจำนวนหุ้นใน portfolio ของ account นั้นๆ
            account_portfolios[account_no][sec_symbol] = (
                account_portfolios[account_no].get(sec_symbol, 0) + trading_unit
            )

        # คำนวณ trading_amt (มูลค่าการซื้อขาย)
        trading_amt = round(trading_unit * trading_price, 2)
        
        # สร้าง order_no ที่ไม่ซ้ำกัน
        order_no = f'ORD{order_counter:07d}'
        order_counter += 1

        # เพิ่มรายการซื้อขายนี้ลงใน list
        all_transactions.append({
            'type_of_order': type_of_order,
            'order_no': order_no,
            'account_no': account_no,
            'sec_symbol': sec_symbol,
            'trading_datetime': trading_datetime,
            'trading_unit': trading_unit,
            'trading_price': trading_price,
            'trading_amt': trading_amt
        })

# --- สร้าง DataFrame จากข้อมูลที่ได้ ---
df = pd.DataFrame(all_transactions)

# --- ตรวจสอบเงื่อนไขที่สำคัญ (เพื่อยืนยันว่าข้อมูลถูกต้องตามที่ต้องการ) ---
print("--- ข้อมูลตัวอย่าง 5 แถวแรก ---")
print(df.head())

print("\n--- สรุปข้อมูลที่สร้างได้ ---")
# ตรวจสอบจำนวน Account ทั้งหมด
print(f"จำนวน Account ทั้งหมด: {df['account_no'].nunique()}")
# ตรวจสอบจำนวนวันที่มีการซื้อขาย
print(f"จำนวนวันที่มีการซื้อขาย: {df['trading_datetime'].dt.date.nunique()}")

# ตรวจสอบว่าแต่ละ Account มีการซื้อขายมากกว่า 1 ครั้งหรือไม่
# กรองเฉพาะ account ที่มีจำนวนรายการมากกว่า 1
accounts_with_multiple_trades = df.groupby('account_no').filter(lambda x: len(x) > 1)['account_no'].nunique()
print(f"จำนวน Account ที่มีการซื้อขายมากกว่า 1 ครั้ง: {accounts_with_multiple_trades} (ควรเท่ากับ {num_accounts})")

# ตรวจสอบ Trading Unit ที่หารด้วย 100 ลงตัว
# ถ้ามีค่าเป็น False แสดงว่าทุกค่าหารด้วย 100 ลงตัว
print(f"มี Trading Unit ที่ไม่หารด้วย 100 ลงตัวหรือไม่: {(df['trading_unit'] % 100 != 0).any()}")

# ตรวจสอบว่ามีรายการขายที่ไม่มีหุ้นในพอร์ตหรือไม่ (ควรเป็น False หากโค้ดทำงานถูกต้อง)
# Note: This check is more complex as it requires re-simulating or carefully checking logs.
# For simplicity, the current code prevents such sales by converting them to BUYs.
# A more robust check would involve logging attempted sales without sufficient stock.
print("การขายถูกจำลองโดยการตรวจสอบพอร์ตก่อน หากไม่มีหุ้นเพียงพอจะถูกเปลี่ยนเป็นคำสั่งซื้อ")

# --- สามารถบันทึกข้อมูลเป็นไฟล์ CSV ได้ (ถ้าต้องการ) ---
df.to_csv('mock_stock_transactions.csv', index=False)
# print("\nข้อมูลถูกบันทึกเป็น 'mock_stock_transactions.csv' แล้ว")
