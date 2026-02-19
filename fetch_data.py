"""
Bitcoin Indicators - Data Fetcher
- Bitcoin price from CoinGecko (12 months live)
- Bitcoin historical price from BTC_USD.csv (2014~)
- Fear & Greed from alternative.me (max available ~2500 days)
- DVOL (VIX) from Deribit
- Put/Call Ratio from Yahoo Finance (IBIT ETF)
Generates: data.json (live data) + btc_history.json (full CSV history)
"""

import json
import csv
import requests
from datetime import datetime, timedelta
import os

# Configuration
DATA_FILE = 'data.json'
BTC_HISTORY_FILE = 'btc_history.json'
BTC_CSV_FILE = 'BTC_USD.csv'


def load_existing_data():
    """Load existing data.json to preserve accumulated PCR history"""
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return None


def generate_btc_history_json():
    """Convert BTC_USD.csv to btc_history.json for frontend use"""
    if not os.path.exists(BTC_CSV_FILE):
        print(f"[INFO] {BTC_CSV_FILE} not found, skipping historical data generation")
        return False

    print(f"[CSV] Reading {BTC_CSV_FILE}...")

    prices = {}
    with open(BTC_CSV_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date = row['Date'].strip()
            try:
                close = round(float(row['Close']), 2)
                prices[date] = close
            except (ValueError, KeyError):
                continue

    with open(BTC_HISTORY_FILE, 'w') as f:
        json.dump(prices, f)

    dates = sorted(prices.keys())
    print(f"   [OK] Generated {BTC_HISTORY_FILE}: {len(prices)} days ({dates[0]} ~ {dates[-1]})")
    return True


def fetch_bitcoin_price():
    """Fetch 12 months of daily Bitcoin price from CoinGecko"""

    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

    params = {
        'vs_currency': 'usd',
        'days': 365,
        'interval': 'daily'
    }

    print("[API] Fetching BTC price from CoinGecko (12 months)...")

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    prices_by_date = {}

    for item in data['prices']:
        timestamp = item[0] / 1000
        date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        price = item[1]
        prices_by_date[date] = round(price, 2)

    print(f"   [OK] Got {len(prices_by_date)} days of price data")

    return prices_by_date


def fetch_fear_greed():
    """Fetch Fear & Greed Index from alternative.me (max available)"""

    url = "https://api.alternative.me/fng/"

    # Request maximum available data (goes back to ~Feb 2018)
    params = {
        'limit': 0,
        'format': 'json'
    }

    print("[API] Fetching Fear & Greed Index from alternative.me (all available)...")

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

        print(f"   [OK] Got {len(fng_by_date)} days of Fear & Greed data")
        if fng_by_date:
            dates = sorted(fng_by_date.keys())
            print(f"   [OK] Range: {dates[0]} ~ {dates[-1]}")
        return fng_by_date

    except Exception as e:
        print(f"   [ERR] Error fetching Fear & Greed: {e}")
        return {}


def fetch_deribit_dvol():
    """Fetch DVOL (Bitcoin Volatility Index) from Deribit"""

    print("[API] Fetching DVOL (VIX) from Deribit...")

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
                timestamp = item[0] / 1000
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                close_value = item[4]
                dvol_by_date[date] = round(close_value, 2)

        print(f"   [OK] Got {len(dvol_by_date)} days of DVOL data")
        return dvol_by_date

    except Exception as e:
        print(f"   [ERR] Error fetching DVOL: {e}")
        return {}


def fetch_ibit_put_call_ratio():
    """Fetch today's IBIT Put/Call ratio from Yahoo Finance using yfinance"""

    print("[API] Fetching IBIT Put/Call ratio from Yahoo Finance...")

    try:
        import yfinance as yf

        ticker = yf.Ticker("IBIT")
        expirations = ticker.options

        if not expirations:
            print("   [ERR] No options data available for IBIT")
            return None

        total_put_oi = 0
        total_call_oi = 0

        for exp in expirations:
            try:
                opt = ticker.option_chain(exp)

                if hasattr(opt, 'calls') and not opt.calls.empty:
                    call_oi = opt.calls['openInterest'].fillna(0).sum()
                    total_call_oi += call_oi

                if hasattr(opt, 'puts') and not opt.puts.empty:
                    put_oi = opt.puts['openInterest'].fillna(0).sum()
                    total_put_oi += put_oi

            except Exception as e:
                continue

        if total_call_oi == 0:
            print("   [ERR] No call open interest data")
            return None

        pcr = round(total_put_oi / total_call_oi, 4)

        print(f"   [OK] Put OI: {total_put_oi:,}, Call OI: {total_call_oi:,}")
        print(f"   [OK] Put/Call Ratio: {pcr}")

        return pcr

    except ImportError:
        print("   [ERR] yfinance not installed. Run: pip install yfinance")
        return None
    except Exception as e:
        print(f"   [ERR] Error fetching IBIT PCR: {e}")
        return None


def align_data(btc_prices, fng_index, vix_index):
    """Align all data to union of all dates (not just BTC)"""

    # Union of ALL dates from all sources
    all_dates_set = set()
    all_dates_set.update(btc_prices.keys())
    all_dates_set.update(fng_index.keys())
    all_dates_set.update(vix_index.keys())
    all_dates = sorted(all_dates_set)

    final_dates = []
    final_prices = []
    final_fng = []
    final_vix = []

    for date in all_dates:
        final_dates.append(date)

        # BTC price - direct or None (frontend fills from btc_history.json)
        final_prices.append(btc_prices.get(date, None))

        # Fear & Greed - direct or nearest within 3 days
        if date in fng_index:
            final_fng.append(fng_index[date])
        else:
            nearest = find_nearest_value(date, fng_index, 3)
            final_fng.append(nearest)

        # VIX/DVOL - direct or nearest within 3 days
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


def update_pcr_history(existing_data, today_pcr):
    """Update PCR history with today's value"""

    today = datetime.now().strftime('%Y-%m-%d')

    if existing_data and 'pcr_dates' in existing_data and 'pcr_index' in existing_data:
        pcr_dates = existing_data['pcr_dates']
        pcr_values = existing_data['pcr_index']
    else:
        pcr_dates = []
        pcr_values = []

    if today_pcr is not None:
        if today not in pcr_dates:
            pcr_dates.append(today)
            pcr_values.append(today_pcr)
            print(f"[PCR] Added PCR for {today}: {today_pcr}")
        else:
            idx = pcr_dates.index(today)
            pcr_values[idx] = today_pcr
            print(f"[PCR] Updated PCR for {today}: {today_pcr}")

    if len(pcr_dates) > 365:
        pcr_dates = pcr_dates[-365:]
        pcr_values = pcr_values[-365:]

    return pcr_dates, pcr_values


def main():
    print("=== Bitcoin Indicators Data Fetch ===\n")

    existing_data = load_existing_data()

    # Generate btc_history.json from CSV
    generate_btc_history_json()

    # Fetch Bitcoin price (12 months live)
    btc_prices = fetch_bitcoin_price()

    if not btc_prices:
        print("[ERR] Could not fetch Bitcoin price")
        return

    # Fetch Fear & Greed Index (all available history)
    fng_index = fetch_fear_greed()

    # Fetch DVOL (VIX)
    vix_index = fetch_deribit_dvol()

    # Fetch IBIT Put/Call Ratio
    today_pcr = fetch_ibit_put_call_ratio()

    # Align data
    print("\n[MERGE] Aligning data...")
    dates, prices, fng, vix = align_data(btc_prices, fng_index, vix_index)
    print(f"   [OK] Aligned {len(dates)} data points")

    # Update PCR history
    pcr_dates, pcr_values = update_pcr_history(existing_data, today_pcr)
    print(f"   [OK] PCR history: {len(pcr_dates)} data points")

    # Save to JSON
    output = {
        'dates': dates,
        'btc_prices': prices,
        'fng_index': fng,
        'vix_index': vix,
        'pcr_dates': pcr_dates,
        'pcr_index': pcr_values,
        'last_updated': datetime.utcnow().isoformat() + 'Z'
    }

    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n[SAVE] Saved to {DATA_FILE}")
    print(f"   Date range: {dates[0]} to {dates[-1]}")
    # Find latest non-null price
    latest_price = next((p for p in reversed(prices) if p is not None), 0)
    print(f"   Latest BTC: ${latest_price:,.0f}")

    latest_fng = fng[-1] if fng[-1] is not None else 'N/A'
    latest_vix = vix[-1] if vix[-1] is not None else 'N/A'
    latest_pcr = pcr_values[-1] if pcr_values else 'N/A'
    print(f"   Latest Fear & Greed: {latest_fng}")
    print(f"   Latest DVOL: {latest_vix}")
    print(f"   Latest Put/Call: {latest_pcr}")


if __name__ == '__main__':
    main()
