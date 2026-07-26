import os
from dotenv import load_dotenv
import ccxt

# 1. Force load the .env file from the current directory
load_dotenv()

# Or import your existing config module:
import config

api_key = config.BINANCE_API_KEY.strip() if config.BINANCE_API_KEY else ""
api_secret = config.BINANCE_API_SECRET.strip() if config.BINANCE_API_SECRET else ""

print(f"Testing with: Key='{api_key[:6]}...' | Testnet Mode={config.TESTNET}")

if not api_key or not api_secret:
    print("❌ ERROR: API Key or Secret is missing! Check your .env file or environment variables.")
    exit(1)

# Initialize CCXT Binance USD-M
exchange = ccxt.binanceusdm({
    "apiKey": api_key,
    "secret": api_secret,
    "enableRateLimit": True,
})

if config.TESTNET:
    exchange.enableDemoTrading(True)

try:
    balance = exchange.fetch_balance()
    print("✅ SUCCESS! Connected to Binance Futures.")
    print("USDT Balance:", balance.get("USDT", {}).get("total", 0))
except Exception as e:
    print("❌ ERROR:", e)