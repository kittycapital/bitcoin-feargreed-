"""
Bitcoin Indicators - Data Fetcher
- Bitcoin price from CoinGecko
- Fear & Greed from alternative.me
- DVOL (VIX) from Deribit
12 months of data
"""

import json
import requests
from datetime import datetime, timedelta

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
    
    try:
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
        
    except Exception as e:
        print(f"   ❌ Error fetching Fear & Greed: {e}")
        return {}


def fetch_deribit_dvol():
    """Fetch DVOL (Bitcoin Volatility Index) from Deribit"""
    
    print("\n📊 Fetching DVOL (VIX) from Deribit...")
    
    # Calculate timestamps for last 12 months
    end_time = datetime.now()
    start_time = end_time - timedelta(days=365)
    
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)
    
    url = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
    
    params = {
        'currency': 'BTC',
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp,
        'resolution': '1D'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        dvol_by_date = {}
        
        if 'result' in data and 'data' in data['result']:
            for item in data['result']['data']:
                # item format: [timestamp, open, high, low, close]
                timestamp = item[0] / 1000
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                close_value = item[4]  # Use close value
                dvol_by_date[date] = round(close_value, 2)
        
        print(f"   ✅ Got {len(dvol_by_date)} days of DVOL data")
        return dvol_by_date
        
    except Exception as e:
        print(f"   ❌ Error fetching DVOL: {e}")
        return {}


def align_data(btc_prices, fng_index, vix_index):
    """Align all data to common dates"""
    
    # Get all unique dates from BTC prices
    all_dates = sorted(btc_prices.keys())
    
    final_dates = []
    final_prices = []
    final_fng = []
    final_vix = []
    
    for date in all_dates:
        final_dates.append(date)
        final_prices.append(btc_prices[date])
        
        # Fear & Greed - use value if exists, otherwise None
        if date in fng_index:
            final_fng.append(fng_index[date])
        else:
            # Try to find nearest date within 3 days
            nearest = find_nearest_value(date, fng_index, 3)
            final_fng.append(nearest)
        
        # VIX/DVOL - use value if exists, otherwise None
        if date in vix_index:
            final_vix.append(vix_index[date])
        else:
            nearest = find_nearest_value(date, vix_index, 3)
            final_vix.append(nearest)
    
    return final_dates, final_prices, final_fng, final_vix


def find_nearest_value(target_date, data_dict, max_days):
    """Find nearest value within max_days"""
    if not data_dict:
        return None
        
    target = datetime.strptime(target_date, '%Y-%m-%d')
    
    nearest_value = None
    min_diff = max_days + 1
    
    for date_str, value in data_dict.items():
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        diff = abs((target - date_obj).days)
        if diff < min_diff:
            min_diff = diff
            nearest_value = value
    
    return nearest_value


def main():
    print("🚀 Starting Bitcoin Indicators data fetch...\n")
    
    # Fetch Bitcoin price
    btc_prices = fetch_bitcoin_price()
    
    if not btc_prices:
        print("❌ Error: Could not fetch Bitcoin price")
        return
    
    # Fetch Fear & Greed Index
    fng_index = fetch_fear_greed()
    
    # Fetch DVOL (VIX)
    vix_index = fetch_deribit_dvol()
    
    # Align data
    print("\n🔄 Aligning data...")
    dates, prices, fng, vix = align_data(btc_prices, fng_index, vix_index)
    print(f"   ✅ Aligned {len(dates)} data points")
    
    # Save to JSON
    output = {
        'dates': dates,
        'btc_prices': prices,
        'fng_index': fng,
        'vix_index': vix,
        'last_updated': datetime.utcnow().isoformat() + 'Z'
    }
    
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Saved to {DATA_FILE}")
    print(f"   📅 Date range: {dates[0]} to {dates[-1]}")
    print(f"   💰 Latest BTC: ${prices[-1]:,.0f}")
    
    # Show latest values
    latest_fng = fng[-1] if fng[-1] is not None else 'N/A'
    latest_vix = vix[-1] if vix[-1] is not None else 'N/A'
    print(f"   😱 Latest Fear & Greed: {latest_fng}")
    print(f"   📈 Latest DVOL: {latest_vix}")


if __name__ == '__main__':
    main()
