import requests

def get_binance_futures_symbols():
    # Endpoints for USDⓈ-M and COIN-M Futures
    usdt_futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    coin_futures_url = "https://dapi.binance.com/dapi/v1/exchangeInfo"
    
    symbols = []
    
    # 1. Fetch USDⓈ-M Futures
    try:
        response = requests.get(usdt_futures_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        usdt_symbols = [
            item['symbol'] for item in data['symbols'] 
            if item.get('status') == 'TRADING'
        ]
        symbols.extend(usdt_symbols)
        print(f"Retrieved {len(usdt_symbols)} active USDⓈ-M Futures symbols.")
    except Exception as e:
        print(f"Error fetching USDⓈ-M Futures: {e}")

    # 2. Fetch COIN-M Futures
    try:
        response = requests.get(coin_futures_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        coin_symbols = [
            item['symbol'] for item in data['symbols'] 
            if item.get('contractStatus') == 'TRADING' or item.get('status') == 'TRADING'
        ]
        symbols.extend(coin_symbols)
        print(f"Retrieved {len(coin_symbols)} active COIN-M Futures symbols.")
    except Exception as e:
        print(f"Error fetching COIN-M Futures: {e}")

    # 3. Sort and remove potential duplicates
    unique_symbols = sorted(list(set(symbols)))
    
    # 4. Save to txt file
    output_filename = "binance_futures_symbols.txt"
    with open(output_filename, "w") as f:
        for symbol in unique_symbols:
            f.write(f"{symbol}\n")
            
    print(f"Saved total {len(unique_symbols)} unique symbols to '{output_filename}'.")

if __name__ == "__main__":
    get_binance_futures_symbols()