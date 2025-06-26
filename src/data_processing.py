# src/data_processing.py

import pandas as pd
import requests
import io
from datetime import datetime, timedelta

def load_full_history_from_storage(s3_client, bucket_name, path):
    """
    Carica tutti i file .parquet dalla cartella raw-history dello storage,
    li unisce e restituisce un unico DataFrame.
    """
    print(f"Caricamento dati storici da s3://{bucket_name}/{path}...")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=path)
        
        df_list = []
        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
                        # Assumiamo che il nome del ticker sia nel nome del file, es. 'TICKER.parquet'
                        df['ticker'] = obj['Key'].replace(path, '').replace('.parquet', '')
                        df_list.append(df)
        
        if not df_list:
            raise FileNotFoundError("Nessun file storico trovato nello storage. Eseguire prima il 'full_refresh'.")

        full_df = pd.concat(df_list, ignore_index=True)
        full_df['date'] = pd.to_datetime(full_df['date'])
        return full_df

    except Exception as e:
        print(f"Errore durante il caricamento della storia da S3: {e}")
        raise

def fetch_daily_delta(tickers, api_key):
    """
    Scarica gli ultimi 3 giorni di dati per la lista di ticker fornita.
    """
    print(f"Scaricamento aggiornamenti per {len(tickers)} tickers...")
    all_deltas = []
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=3)
    
    for ticker in tickers:
        # La struttura dell'URL può variare, questo è un esempio
        url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&from={start_date}&to={end_date}&fmt=json&period=d"
        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            if data:
                df = pd.DataFrame(data)
                df['ticker'] = ticker
                all_deltas.append(df)
        except requests.exceptions.RequestException as e:
            # Ignora errori per singoli ticker, ma logga il problema
            print(f"Attenzione: Impossibile scaricare l'aggiornamento per {ticker}. Errore: {e}")
            continue

    if not all_deltas:
        return pd.DataFrame()
        
    delta_df = pd.concat(all_deltas, ignore_index=True)
    # Assicurati che le colonne corrispondano al formato storico
    delta_df = delta_df.rename(columns={'adjusted_close': 'close'}) # Esempio di rinomina
    delta_df['date'] = pd.to_datetime(delta_df['date'])
    return delta_df[['date', 'close', 'volume', 'ticker']] # Seleziona solo le colonne necessarie

def create_dynamic_baskets(df, top_n=50, lookback_days=30):
    """
    Crea i panieri dinamici basati sul volume come proxy del market cap.
    """
    df_with_dates = df.set_index('date')
    
    # Determina le date di rebalancing (es. fine di ogni 'stagione' di 90 giorni)
    start_date = df_with_dates.index.min()
    end_date = df_with_dates.index.max()
    rebalancing_dates = pd.date_range(start=start_date, end=end_date, freq='90D')
    
    baskets = {}
    for reb_date in rebalancing_dates:
        # Definisci il periodo di lookback per calcolare il volume
        lookback_start = reb_date - pd.Timedelta(days=lookback_days)
        
        # Filtra i dati per il periodo di lookback
        volume_period_df = df_with_dates.loc[lookback_start:reb_date]
        
        if volume_period_df.empty:
            continue
            
        # Calcola il volume totale per ticker nel periodo
        total_volume = volume_period_df.groupby('ticker')['volume'].sum()
        
        # Seleziona i top N ticker escludendo BTC
        btc_ticker = next((t for t in total_volume.index if 'BTC' in t), None)
        top_altcoins = total_volume.drop(labels=[btc_ticker] if btc_ticker else []).nlargest(top_n)
        
        baskets[reb_date] = top_altcoins.index.tolist()
        
    return baskets

def calculate_full_asi(df, baskets, performance_window=90):
    """
    Calcola l'Altcoin Season Index usando i panieri dinamici.
    """
    df_with_dates = df.set_index('date')
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC' in t), None)
    
    if not btc_ticker:
        raise ValueError("Ticker di Bitcoin non trovato nel dataset.")
        
    btc_perf = df_with_dates[df_with_dates['ticker'] == btc_ticker]['close'].pct_change(periods=performance_window)
    
    # Pre-calcola le performance di tutte le altcoin
    alt_perf = df_with_dates[df_with_dates['ticker'] != btc_ticker].groupby('ticker')['close'].pct_change(periods=performance_window)
    
    all_dates = df_with_dates.index.unique().sort_values()
    rebalancing_dates = sorted(baskets.keys())
    
    asi_results = []
    
    for eval_date in all_dates:
        # Trova il paniere attivo per la data di valutazione
        active_basket_date = next((rd for rd in reversed(rebalancing_dates) if rd <= eval_date), None)
        if not active_basket_date:
            continue
            
        active_basket = baskets[active_basket_date]
        
        # Filtra le performance per il paniere e la data attuali
        current_btc_perf = btc_perf.get(eval_date, np.nan)
        if pd.isna(current_btc_perf):
            continue
            
        current_alt_perf = alt_perf.loc[eval_date]
        
        # Confronta le performance
        outperforming_count = 0
        valid_alts_in_basket = 0
        for alt in active_basket:
            perf = current_alt_perf.get(alt, np.nan)
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

    result_df = pd.DataFrame(asi_results).set_index('date')
    return result_df
