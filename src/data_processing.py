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
    start_date = end_date - timedelta(days=5) # Lookback di 5 giorni per sicurezza
    
    for ticker in tickers:
        url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&from={start_date}&to={end_date}&fmt=json&period=d"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data:
                    df = pd.DataFrame(data)
                    
                    # --- INIZIO DELLA SOLUZIONE ---
                    # 1. Prioritizza 'adjusted_close' e rinominalo in 'close'.
                    #    Se non esiste, il 'close' originale verrà usato.
                    if 'adjusted_close' in df.columns:
                        df['close'] = df['adjusted_close']
                    
                    # 2. Seleziona esplicitamente un set di colonne univoche.
                    #    Questo elimina automaticamente qualsiasi colonna duplicata ('close' originale) o non necessaria.
                    required_cols = ['date', 'close', 'volume']
                    
                    # Controlla se le colonne necessarie esistono prima di procedere
                    if not all(col in df.columns for col in required_cols):
                        print(f"  - Dati incompleti per {ticker}. Salto.")
                        continue
                        
                    final_df = df[required_cols].copy() # Usa .copy() per evitare warning
                    # --- FINE DELLA SOLUZIONE ---

                    final_df.dropna(subset=['date', 'close', 'volume'], inplace=True)
                    if not final_df.empty:
                        final_df['date'] = pd.to_datetime(final_df['date'])
                        final_df.set_index('date', inplace=True)
                        delta_dict[ticker] = final_df[['close', 'volume']] # La selezione finale avviene qui

        except requests.exceptions.RequestException as e:
            print(f"  - Errore API per {ticker}: {e}. Salto.")
            continue
        time.sleep(0.1)

    return delta_dict if delta_dict else None

def create_dynamic_baskets(df: pd.DataFrame, top_n: int = 50, lookback_days: int = 30, rebalancing_freq: str = '90D', performance_window: int = 90) -> Dict:
    if df.empty: return {}
        
    # --- INIZIO SOLUZIONE ---
    
    # 1. Assicura che la colonna 'date' sia in formato datetime corretto.
    df['date'] = pd.to_datetime(df['date'])

    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    
    # 2. NON impostare più la data come indice del DataFrame aggregato.
    # df_with_dates = df.set_index('date') <-- RIMUOVI O COMMENTA QUESTA RIGA

    start_date = pd.to_datetime('2017-01-01')
    end_date = df['date'].max() # Usa la colonna 'date'
    rebalancing_dates = pd.date_range(start=start_date, end=end_date, freq=rebalancing_freq)
    
    baskets = {}
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC-USD.CC' in t), None)

    for reb_date in rebalancing_dates:
        lookback_start = reb_date - pd.Timedelta(days=lookback_days)

        # 3. Usa il boolean indexing sulla colonna 'date' invece dello slicing su .loc.
        mask = (df['date'] > lookback_start) & (df['date'] <= reb_date)
        volume_period_df = df.loc[mask]
        
        # --- FINE SOLUZIONE ---

        if volume_period_df.empty: continue
            
        total_volume = volume_period_df.groupby('ticker')['volume'].sum()
        if btc_ticker and btc_ticker in total_volume.index:
            altcoin_volumes = total_volume.drop(btc_ticker, errors='ignore')
        else:
            altcoin_volumes = total_volume
            
        top_by_volume = altcoin_volumes.nlargest(top_n)
        
        if btc_ticker and btc_ticker in total_volume.index:
            altcoin_volumes = total_volume.drop(btc_ticker, errors='ignore')
        else:
            altcoin_volumes = total_volume
            
        top_by_volume = altcoin_volumes.nlargest(top_n)
        
        # --- SOLUZIONE ---
        # Seleziona direttamente i ticker dalla lista top_by_volume senza ulteriori filtri,
        # proprio come faceva il notebook originale.
        final_basket_coins = top_by_volume.index.tolist()
        
        if final_basket_coins:
            baskets[reb_date] = final_basket_coins
                
    return baskets

def calculate_full_asi(data_dict: Dict[str, pd.DataFrame], baskets: Dict, performance_window: int = 90) -> pd.DataFrame:
    if not data_dict or not baskets: return pd.DataFrame()

    btc_ticker = next((t for t in data_dict.keys() if 'BTC-USD.CC' in t), None)
    if not btc_ticker or btc_ticker not in data_dict: raise ValueError("Dati di Bitcoin non trovati.")
    
    all_dates = pd.to_datetime(sorted(list(set(date for df in data_dict.values() for date in df.index))))
    rebalancing_dates = sorted(baskets.keys())
    
    perf_dict = {ticker: df['close'].pct_change(periods=performance_window) for ticker, df in data_dict.items()}
    
    asi_results = []
    start_eval_date = all_dates.min() + timedelta(days=performance_window)
    
    for eval_date in all_dates[all_dates >= start_eval_date]:
        active_basket_date = next((rd for rd in reversed(rebalancing_dates) if rd <= eval_date), None)
        if not active_basket_date: continue
            
        active_basket = baskets.get(active_basket_date, [])
        current_btc_perf = perf_dict[btc_ticker].get(eval_date)
        
        if pd.isna(current_btc_perf): continue

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
        return pd.DataFrame(columns=['index_value', 'outperforming_count', 'basket_size'])
        
    return pd.DataFrame(asi_results).set_index('date')
