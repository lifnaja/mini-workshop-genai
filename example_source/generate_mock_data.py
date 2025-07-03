import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --- 1. กำหนด Master Data และพารามิเตอร์ ---
# Number of accounts to generate
NUM_ACCOUNTS = 200
# Number of days for trading data
NUM_DAYS = 30
# Start date for the trading data (example: June 1, 2025)
START_DATE = datetime.strptime("2025-06-01", "%Y-%m-%d")

# List of stock symbols and their approximate price ranges
# You can add more stocks and adjust price ranges as needed
STOCK_PRICES = {
    "AOT": {"min": 60, "max": 75},
    "ADVANC": {"min": 200, "max": 230},
    "BBL": {"min": 130, "max": 150},
    "BDMS": {"min": 25, "max": 30},
    "CPALL": {"min": 50, "max": 60},
    "CRC": {"min": 30, "max": 38},
    "DELTA": {"min": 80, "max": 95},
    "GULF": {"min": 40, "max": 50},
    "INTUCH": {"min": 70, "max": 80},
    "KBANK": {"min": 130, "max": 150},
    "KTB": {"min": 17, "max": 20},
    "PTT": {"min": 30, "max": 38},
    "SCB": {"min": 110, "max": 130},
    "SCC": {"min": 250, "max": 280},
    "TISCO": {"min": 90, "max": 100},
    "TOP": {"min": 50, "max": 60},
    "TRUE": {"min": 5, "max": 7},
    "OSP": {"min": 20, "max": 25},
    "KTC": {"min": 45, "max": 55},
    "MINT": {"min": 30, "max": 38}
}
SEC_SYMBOLS = list(STOCK_PRICES.keys())

all_transactions = [] # List to store all generated transactions
# Dictionary to keep track of each account's portfolio: {account_no: {sec_symbol: total_unit}}
account_portfolios = {} 

# --- 2. สร้างบัญชีผู้ใช้งานและจำลองพอร์ตเริ่มต้น ---
# Generate a list of unique account numbers
account_list = [f"ACC{str(i+1).zfill(3)}" for i in range(NUM_ACCOUNTS)]

# Initialize each account's portfolio with some random initial stocks
for acc_no in account_list:
    account_portfolios[acc_no] = {}
    # Randomly add 3-5 initial stocks to each portfolio
    initial_stocks = random.sample(SEC_SYMBOLS, k=random.randint(3, 5))
    for stock in initial_stocks:
        # Random initial unit (must be divisible by 100)
        initial_unit = random.randint(1, 10) * 100
        account_portfolios[acc_no][stock] = initial_unit

# --- 3. จำลองการซื้อขายรายวัน ---
# Loop through each day for the specified number of days
for day in range(NUM_DAYS):
    current_date = START_DATE + timedelta(days=day)
    
    # Randomly select a subset of accounts that will have trades on this day
    # Not all accounts will trade every day
    active_accounts_today = random.sample(account_list, k=random.randint(NUM_ACCOUNTS // 2, NUM_ACCOUNTS))
    
    # For each active account, generate trades
    for acc_no in active_accounts_today:
        # Each account must have more than 1 trade (randomly 1-5 trades per day)
        num_trades_today = random.randint(1, 5) 
        
        for _ in range(num_trades_today):
            type_of_order = random.choice(["BUY", "SELL"])
            sec_symbol = random.choice(SEC_SYMBOLS)
            
            # Randomize time within the current day
            trading_datetime = current_date.replace(
                hour=random.randint(9, 16), # Trading hours 9 AM - 4 PM
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            # Get price range for the selected stock
            min_price = STOCK_PRICES[sec_symbol]["min"]
            max_price = STOCK_PRICES[sec_symbol]["max"]
            # Generate a random trading price within the defined range
            trading_price = round(random.uniform(min_price, max_price), 2)
            
            # Handle SELL orders
            if type_of_order == "SELL":
                # Check if the account has the stock and if the quantity is greater than 0
                if sec_symbol in account_portfolios[acc_no] and account_portfolios[acc_no][sec_symbol] > 0:
                    available_units = account_portfolios[acc_no][sec_symbol]
                    # Calculate maximum tradeable units (must be divisible by 100)
                    max_trade_unit = (available_units // 100) * 100
                    if max_trade_unit == 0: # If stock exists but less than 100 units
                        continue # Skip this trade
                    
                    # Randomly select units to sell (divisible by 100 and not exceeding available units)
                    trading_unit = random.randint(1, max_trade_unit // 100) * 100
                    if trading_unit == 0: # Prevent 0 units if max_trade_unit is small
                        continue 
                        
                    # Update portfolio after selling
                    account_portfolios[acc_no][sec_symbol] -= trading_unit
                else:
                    # If no stock in portfolio, cannot sell - skip this transaction
                    continue 
            # Handle BUY orders
            else: 
                # Randomly select units to buy (must be divisible by 100)
                trading_unit = random.randint(1, 100) * 100 
                
                # Update portfolio after buying (add to existing or initialize)
                account_portfolios[acc_no][sec_symbol] = account_portfolios[acc_no].get(sec_symbol, 0) + trading_unit
            
            # Calculate trading amount
            trading_amt = round(trading_unit * trading_price, 2)
            
            # Create a unique order number using timestamp and a random suffix
            transaction = {
                "type_of_order": type_of_order,
                "order_no": f"ORD{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{random.randint(1000, 9999)}", 
                "account_no": acc_no,
                "sec_symbol": sec_symbol,
                "trading_datetime": trading_datetime,
                "trading_unit": trading_unit,
                "trading_price": trading_price,
                "trading_amt": trading_amt
            }
            all_transactions.append(transaction)

# Create a Pandas DataFrame from the generated transactions
df_transactions = pd.DataFrame(all_transactions)

# Sort the DataFrame by trading_datetime for better chronological order
df_transactions = df_transactions.sort_values(by="trading_datetime").reset_index(drop=True)

# Print summary of the generated data
print(f"Generated {len(df_transactions)} trading records.")
print("\nFirst 5 rows of the generated data:")
print(df_transactions.head())
print("\nSummary statistics:")
print(df_transactions.describe())
print(f"\nNumber of unique accounts with transactions: {df_transactions['account_no'].nunique()}")
print(f"Number of unique trading days: {df_transactions['trading_datetime'].dt.date.nunique()}")
print("\nExample of final portfolio for some accounts:")


df_transactions.to_csv("sample.csv", index=False)