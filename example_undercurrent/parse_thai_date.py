from datetime import datetime

import pandas as pd


def parse_thai_date(date_str):
    thai_months = {
        'ม.ค.': '01', 'ก.พ.': '02', 'มี.ค.': '03', 'เม.ย.': '04',
        'พ.ค.': '05', 'มิ.ย.': '06', 'ก.ค.': '07', 'ส.ค.': '08',
        'ก.ย.': '09', 'ต.ค.': '10', 'พ.ย.': '11', 'ธ.ค.': '12'
    }
    try:
        day, month_th, year_th = date_str.strip().split()
        month = thai_months.get(month_th)
        year = int(year_th) - 543  # แปลง พ.ศ. เป็น ค.ศ.
        return datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y")
    except Exception:
        return pd.NaT  # ถ้าแปลงไม่ได้ให้คืนค่า NaT (Not a Time)
