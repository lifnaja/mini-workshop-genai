import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://coinmarketcap.com/"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# ค้นหาแถวของเหรียญ
rows = soup.select("table tbody tr")

coins = []

for row in rows:
    print("row")
    print(row)
    try:
        name = row.select_one('p.sc-4984dd93-0.kKpPOn').text
        symbol = row.select_one('p[class*="coin-item-symbol"]').text
        price = row.select_one('span[class*="price"]').text
        change_24h = row.select('td')[4].text
        market_cap = row.select('td')[6].text

        coins.append({
            "Name": name,
            "Symbol": symbol,
            "Price": price,
            "Change (24h)": change_24h,
            "Market Cap": market_cap
        })
    except Exception as e:
        continue  # บางแถวอาจไม่มีข้อมูลครบ

# แสดงเป็น DataFrame
df = pd.DataFrame(coins)
print(df.head())
