"""
Bitcoin Fear & Greed Index - Data Fetcher
Bitcoin price from CoinGecko + Fear & Greed from alternative.me
12 months of data
"""

import json
import requests
from datetime import datetime

# Configuration
DATA_FILE = 'data.json'


def fetch_bitcoin_price():
    """Fetch 12 months of daily Bitcoin price from CoinGecko"""
    
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    
    params = {
        'vs_currency': 'usd',
        'days': 365,
        'interval': 'daily'
    }
    
    print("📡 Fetching BTC price from CoinGecko (12 months)...")
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    prices_by_date = {}
    
    for item in data['prices']:
        timestamp = item[0] / 1000
        date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        price = item[1]
        prices_by_date[date] = round(price, 2)
    
    print(f"   ✅ Got {len(prices_by_date)} days of price data")
    
    return prices_by_date


def fetch_fear_greed():
    """Fetch Fear & Greed Index from alternative.me"""
    
    url = "https://api.alternative.me/fng/"
    
    params = {
        'limit': 365,
        'format': 'json'
    }
    
    print("\n📊 Fetching Fear & Greed Index from alternative.me...")
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    fng_by_date = {}
    
    if 'data' in data:
        for item in data['data']:
            timestamp = int(item['timestamp'])
            date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            value = int(item['value'])
            fng_by_date[date] = value
    
    print(f"   ✅ Got {len(fng_by_date)} days of Fear & Greed data")
    
    return fng_by_date


def align_data(btc_prices, fng_index):
    """Align Bitcoin prices with Fear & Greed data"""
    
    # Get dates that exist in both datasets
    all_dates = sorted(set(btc_prices.keys()) & set(fng_index.keys()))
    
    final_dates = []
    final_prices = []
    final_fng = []
    
    for date in all_dates:
        final_dates.append(date)
        final_prices.append(btc_prices[date])
        final_fng.append(fng_index[date])
    
    return final_dates, final_prices, final_fng


def main():
    print("🚀 Starting Bitcoin Fear & Greed data fetch...\n")
    
    # Fetch Bitcoin price
    btc_prices = fetch_bitcoin_price()
    
    if not btc_prices:
        print("❌ Error: Could not fetch Bitcoin price")
        return
    
    # Fetch Fear & Greed Index
    fng_index = fetch_fear_greed()
    
    if not fng_index:
        print("❌ Error: Could not fetch Fear & Greed data")
        return
    
    # Align data
    print("\n🔄 Aligning data...")
    dates, prices, fng = align_data(btc_prices, fng_index)
    print(f"   ✅ Aligned {len(dates)} data points")
    
    # Save to JSON
    output = {
        'dates': dates,
        'btc_prices': prices,
        'fng_index': fng,
        'last_updated': datetime.utcnow().isoformat() + 'Z'
    }
    
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Saved to {DATA_FILE}")
    print(f"   📅 Date range: {dates[0]} to {dates[-1]}")
    print(f"   💰 Latest BTC: ${prices[-1]:,.0f}")
    print(f"   😱 Latest Fear & Greed: {fng[-1]}")


if __name__ == '__main__':
    main()
