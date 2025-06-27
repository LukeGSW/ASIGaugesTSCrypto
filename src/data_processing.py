# src/data_processing.py

import pandas as pd
import requests
import numpy as np
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

def fetch_daily_delta(tickers: List[str], api_key: str) -> Optional[pd.DataFrame]:
    """Scarica gli ultimi 3 giorni di dati per la lista di ticker fornita da EODHD."""
    print(f"Scaricamento aggiornamenti per {len(tickers)} tickers...")
    all_deltas = []
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=3)
    
    for ticker in tickers:
        url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&from={start_date}&to={end_date}&fmt=json&period=d"
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status() # Controlla se ci sono errori HTTP (es. 404, 500)
            data = r.json()
            if data:
                df = pd.DataFrame(data)

                # --- INIZIO DELLA CORREZIONE ---

                # 1. Standardizza la colonna 'close', dando priorità ad 'adjusted_close'.
                #    Questo previene la duplicazione della colonna 'close'.
                if 'adjusted_close' in df.columns:
                    df['close'] = df['adjusted_close']

                # 2. Definisci le colonne essenziali e assicurati che esistano.
                required_cols = ['date', 'close', 'volume']
                if not all(col in df.columns for col in required_cols):
                    print(f"Attenzione: Dati essenziali mancanti per {ticker}. Salto.")
                    continue

                # 3. Seleziona solo le colonne necessarie per mantenere la coerenza.
                #    Aggiungi il ticker e appenndi il DataFrame pulito alla lista.
                clean_df = df[required_cols].copy()
                clean_df['ticker'] = ticker
                all_deltas.append(clean_df)

                # --- FINE DELLA CORREZIONE ---

        except requests.exceptions.RequestException as e:
            print(f"Attenzione: Impossibile scaricare l'aggiornamento per {ticker}. Errore: {e}")
            continue
        # Pausa per non sovraccaricare l'API
        time.sleep(0.1) 

    if not all_deltas:
        print("Nessun dato delta è stato scaricato.")
        return None
        
    # Ora la concatenazione è sicura perché tutti i DataFrame hanno la stessa struttura
    delta_df = pd.concat(all_deltas, ignore_index=True)
    delta_df['date'] = pd.to_datetime(delta_df['date'])
    
    return delta_df

# Il resto del file (create_dynamic_baskets, calculate_full_asi) rimane identico
def create_dynamic_baskets(df: pd.DataFrame, top_n: int = 50, lookback_days: int = 30, rebalancing_freq: str = '90D') -> Dict:
    # ... (logica invariata)
    if df.empty:
        return {}
        
    df_with_dates = df.set_index('date')
    start_date = df_with_dates.index.min()
    end_date = df_with_dates.index.max()
    rebalancing_dates = pd.date_range(start=start_date, end=end_date, freq=rebalancing_freq)
    
    baskets = {}
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC' in t), None)

    for reb_date in rebalancing_dates:
        lookback_start = reb_date - pd.Timedelta(days=lookback_days)
        volume_period_df = df_with_dates.loc[lookback_start:reb_date]
        if volume_period_df.empty:
            continue
            
        total_volume = volume_period_df.groupby('ticker')['volume'].sum()
        top_altcoins = total_volume.drop(labels=[btc_ticker] if btc_ticker else []).nlargest(top_n)
        baskets[reb_date] = top_altcoins.index.tolist()
        
    return baskets

def calculate_full_asi(df: pd.DataFrame, baskets: Dict, performance_window: int = 90) -> pd.DataFrame:
    # ... (logica invariata)
    df_with_dates = df.set_index('date')
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC' in t), None)
    if not btc_ticker: raise ValueError("Ticker di Bitcoin non trovato.")
        
    btc_perf = df_with_dates[df_with_dates['ticker'] == btc_ticker]['close'].pct_change(periods=performance_window)
    alt_perf = df_with_dates[df_with_dates['ticker'] != btc_ticker].groupby('ticker')['close'].pct_change(periods=performance_window)
    
    all_dates = df_with_dates.index.unique().sort_values()
    rebalancing_dates = sorted(baskets.keys())
    
    asi_results = []
    
    # Inizia a calcolare solo dopo che ci sia abbastanza storia per la finestra di performance
    start_eval_date = all_dates.min() + timedelta(days=performance_window)
    
    for eval_date in all_dates[all_dates >= start_eval_date]:
        active_basket_date = next((rd for rd in reversed(rebalancing_dates) if rd <= eval_date), None)
        if not active_basket_date: continue
            
        active_basket = baskets[active_basket_date]
        current_btc_perf = btc_perf.get(eval_date, np.nan)
        if pd.isna(current_btc_perf): continue
            
        try:
            current_alt_perf_series = alt_perf.loc[eval_date]
        except KeyError:
            continue

        outperforming_count = 0
        valid_alts_in_basket = 0
        for alt in active_basket:
            perf = current_alt_perf_series.get(alt, np.nan)
            if not pd.isna(perf):
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

    if not asi_results: return pd.DataFrame()
    return pd.DataFrame(asi_results).set_index('date')
