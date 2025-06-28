import pandas as pd
import requests
import numpy as np
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

def fetch_daily_delta(tickers: List[str], api_key: str) -> Optional[Dict[str, pd.DataFrame]]:
    print(f"Scaricamento aggiornamenti per {len(tickers)} tickers...")
    delta_dict = {}
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=5)
    
    for ticker in tickers:
        url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&from={start_date}&to={end_date}&fmt=json&period=d"
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data:
                df = pd.DataFrame(data)
                
                final_data = {}
                if 'date' not in df.columns:
                    print(f"  - Nessuna colonna 'date' per {ticker}. Salto.")
                    continue
                
                final_data['date'] = df['date']
                
                if 'adjusted_close' in df.columns and df['adjusted_close'].notna().any():
                    final_data['close'] = df['adjusted_close']
                elif 'close' in df.columns:
                    final_data['close'] = df['close']
                else:
                    print(f"  - Nessuna colonna 'close' valida per {ticker}. Salto.")
                    continue

                if 'volume' in df.columns:
                    final_data['volume'] = df['volume']
                else:
                    print(f"  - Nessuna colonna 'volume' per {ticker}. Salto.")
                    continue
                
                final_df = pd.DataFrame(final_data)
                final_df.dropna(inplace=True)

                if not final_df.empty:
                    final_df['date'] = pd.to_datetime(final_df['date'], utc=False)
                    final_df.set_index('date', inplace=True)
                    delta_dict[ticker] = final_df[['close', 'volume']]
                else:
                    print(f"  - Dati vuoti dopo pulizia per {ticker}. Salto.")

        except requests.exceptions.RequestException as e:
            print(f"  - Errore API per {ticker}: {e}")
            continue
        time.sleep(0.1)

    return delta_dict if delta_dict else None

def create_dynamic_baskets(df: pd.DataFrame, top_n: int = 50, lookback_days: int = 30, rebalancing_freq: str = '90D', performance_window: int = 90) -> Dict:
    if df.empty:
        print("DataFrame vuoto. Nessun paniere generato.")
        return {}
        
    # Converti le date in tz-naive
    df['date'] = pd.to_datetime(df['date'], utc=False)
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    
    start_date = pd.to_datetime('2018-01-01', utc=False)
    end_date = df['date'].max()
    if end_date.tz is not None:
        end_date = end_date.tz_localize(None)
    
    print(f"Creazione panieri dinamici da {start_date} a {end_date}...")
    rebalancing_dates = pd.date_range(start=start_date, end=end_date, freq=rebalancing_freq)
    
    baskets = {}
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC-USD.CC' in t), None)

    for reb_date in rebalancing_dates:
        lookback_start = reb_date - pd.Timedelta(days=lookback_days)
        
        mask = (df['date'] > lookback_start) & (df['date'] <= reb_date)
        volume_period_df = df.loc[mask]
        
        if volume_period_df.empty:
            print(f"Nessun dato per il periodo {lookback_start} - {reb_date}. Salto.")
            continue
            
        avg_volume = volume_period_df.groupby('ticker')['volume'].mean()
        
        if btc_ticker and btc_ticker in avg_volume.index:
            altcoin_volumes = avg_volume.drop(btc_ticker, errors='ignore')
        else:
            altcoin_volumes = avg_volume
            
        top_by_volume = altcoin_volumes.nlargest(top_n)
        
        final_basket_coins = top_by_volume.index.tolist()
        
        if final_basket_coins:
            baskets[reb_date] = final_basket_coins
        else:
            print(f"Nessun paniere generato per {reb_date}.")
                
    return baskets

def calculate_full_asi(data_dict: Dict[str, pd.DataFrame], baskets: Dict, performance_window: int = 90) -> pd.DataFrame:
    if not data_dict or not baskets:
        print("Dati o panieri vuoti. Restituisco DataFrame vuoto.")
        return pd.DataFrame()

    btc_ticker = next((t for t in data_dict.keys() if 'BTC-USD.CC' in t), None)
    if not btc_ticker or btc_ticker not in data_dict:
        raise ValueError("Dati di Bitcoin non trovati.")
    
    all_dates = pd.to_datetime(sorted(list(set(date for df in data_dict.values() for date in df.index))), utc=False)
    rebalancing_dates = sorted(baskets.keys())
    
    perf_dict = {ticker: df['close'].pct_change(periods=performance_window) for ticker, df in data_dict.items()}
    
    asi_results = []
    
    first_rebal_date = rebalancing_dates[0]
    start_eval_date = first_rebal_date
    
    for eval_date in all_dates[all_dates >= start_eval_date]:
        active_basket_date = next((rd for rd in reversed(rebalancing_dates) if rd <= eval_date), None)
        if not active_basket_date:
            continue
            
        active_basket = baskets.get(active_basket_date, [])
        current_btc_perf = perf_dict[btc_ticker].get(eval_date)
        
        if pd.isna(current_btc_perf):
            continue

        outperforming_count = 0
        valid_alts_in_basket = 0
        for alt in active_basket:
            perf = perf_dict.get(alt, pd.Series(dtype=float)).get(eval_date)
            if pd.notna(perf):
                valid_alts_in_basket += 1
                if perf > current_btc_perf:
                    outperforming_count += 1
        
        if valid_alts_in_basket > 0:
            index_value = (outperforming_count / valid_alts_in_basket) * 100
            asi_results.append({
                'date': eval_date,
                'index_value': index_value,
                'outperforming_count': outperforming_count,
                'basket_size': valid_alts_in_basket
            })

    if not asi_results:
        print("Nessun risultato ASI generato.")
        return pd.DataFrame(columns=['index_value', 'outperforming_count', 'basket_size'])
        
    return pd.DataFrame(asi_results).set_index('date')
