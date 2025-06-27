# src/data_processing.py (VERSIONE DI PRODUZIONE FINALE)

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
    start_date = end_date - timedelta(days=4) # Prendiamo un giorno in più per sicurezza
    
    for ticker in tickers:
        url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&from={start_date}&to={end_date}&fmt=json&period=d"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data:
                    df = pd.DataFrame(data)
                    df['ticker'] = ticker
                    all_deltas.append(df)
        except requests.exceptions.RequestException as e:
            print(f"Attenzione: Impossibile scaricare l'aggiornamento per {ticker}. Errore: {e}")
            continue
        time.sleep(0.1) 

    if not all_deltas:
        return None
        
    delta_df = pd.concat(all_deltas, ignore_index=True)
    if 'adjusted_close' in delta_df.columns:
        delta_df = delta_df.rename(columns={'adjusted_close': 'close'})
    delta_df['date'] = pd.to_datetime(delta_df['date'])
    
    required_cols = ['date', 'close', 'volume', 'ticker']
    return delta_df[[col for col in required_cols if col in delta_df.columns]]

def create_dynamic_baskets(df: pd.DataFrame, top_n: int = 50, lookback_days: int = 30, rebalancing_freq: str = '90D', performance_window: int = 90) -> Dict:
    """Crea i panieri dinamici basati sul volume e sullo storico disponibile."""
    if df.empty:
        return {}
        
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    df_with_dates = df.set_index('date')
    
    start_date = pd.to_datetime('2017-01-01')
    end_date = df_with_dates.index.max()
    rebalancing_dates = pd.date_range(start=start_date, end=end_date, freq=rebalancing_freq)
    
    baskets = {}
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC-USD.CC' in t), None)

    for reb_date in rebalancing_dates:
        lookback_start = reb_date - pd.Timedelta(days=lookback_days)
        volume_period_df = df_with_dates.loc[lookback_start:reb_date]
        
        if volume_period_df.empty:
            continue
            
        total_volume = volume_period_df.groupby('ticker')['volume'].sum()
        
        if btc_ticker and btc_ticker in total_volume.index:
            altcoin_volumes = total_volume.drop(btc_ticker)
        else:
            altcoin_volumes = total_volume
            
        top_by_volume = altcoin_volumes.nlargest(top_n)
        
        final_basket_coins = []
        for ticker in top_by_volume.index:
            ticker_history = df_with_dates.loc[df_with_dates['ticker'] == ticker].loc[:reb_date]
            if len(ticker_history) >= performance_window:
                final_basket_coins.append(ticker)
        
        if final_basket_coins:
            baskets[reb_date] = final_basket_coins
                
    return baskets

def calculate_full_asi(df: pd.DataFrame, baskets: Dict, performance_window: int = 90) -> pd.DataFrame:
    """
    Calcola l'Altcoin Season Index usando un approccio basato su pivot, più robusto e corretto.
    """
    if df.empty or not baskets:
        return pd.DataFrame()

    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC-USD.CC' in t), None)
    if not btc_ticker: raise ValueError("Ticker di Bitcoin non trovato nel dataset.")

    print("  - [ASI Calc] Calcolo performance per tutti i ticker...")
    # Lavoriamo con la struttura dati "long" originale
    df['performance'] = df.groupby('ticker')['close'].pct_change(periods=performance_window)
    
    print("  - [ASI Calc] Creazione tabella pivot delle performance...")
    # Creiamo una tabella "wide" dove le righe sono date e le colonne sono i ticker.
    # Questo risolve tutti i problemi di indicizzazione.
    perf_pivot_df = df.pivot_table(index='date', columns='ticker', values='performance')
    
    rebalancing_dates = sorted(baskets.keys())
    
    # La prima data possibile per una valutazione è la prima data di rebalancing + la finestra di performance
    start_eval_date = rebalancing_dates[0] + timedelta(days=performance_window)
    
    asi_results = []

    print(f"  - [ASI Calc] Inizio ciclo di valutazione da {start_eval_date.strftime('%Y-%m-%d')}...")
    # Iteriamo su tutte le date presenti nella tabella pivot
    for eval_date in perf_pivot_df.index:
        if eval_date < start_eval_date:
            continue

        # Trova il paniere corretto per la data di valutazione
        active_basket_date = next((rd for rd in reversed(rebalancing_dates) if rd <= eval_date), None)
        if not active_basket_date: 
            continue
        
        active_basket = baskets[active_basket_date]
        
        # Estrai le performance del giorno dalla tabella pivot
        btc_perf = perf_pivot_df.at[eval_date, btc_ticker]
        
        # Se BTC non ha una performance valida per quel giorno, saltiamo
        if pd.isna(btc_perf):
            continue

        # Estrai le performance delle altcoin nel paniere attivo
        alt_performances = perf_pivot_df.loc[eval_date, active_basket].dropna()
        
        if alt_performances.empty:
            continue

        outperforming_count = (alt_performances > btc_perf).sum()
        valid_alts_in_basket = len(alt_performances)
        
        index_value = (outperforming_count / valid_alts_in_basket) * 100
        asi_results.append({
            'date': eval_date,
            'index_value': index_value,
            'outperforming_count': outperforming_count,
            'basket_size': valid_alts_in_basket
        })

    if not asi_results: 
        return pd.DataFrame()
        
    final_df = pd.DataFrame(asi_results).set_index('date')
    return final_df
