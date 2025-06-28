# run_full_refresh.py (CORRETTO)

import os
import pandas as pd
import requests
import time
from typing import List, Optional
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
CRYPTO_EXCHANGE_CODE = "CC"
ROOT_FOLDER_NAME = "KriterionQuant_Data"
RAW_HISTORY_FOLDER_NAME = "raw-history"
# --- SOLUZIONE: AGGIUNTA DATA INIZIO ---
START_DATE = "2018-01-01"

def get_all_tickers(api_key: str, exchange_code: str) -> List[str]:
    print(f"Recupero lista ticker per exchange '{exchange_code}'...")
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        
        tickers_set = {
            item['Code'] + '.' + exchange_code 
            for item in data 
            if item.get('Code', '').endswith('-USD')
        }
        
        btc_ticker_name = "BTC-USD.CC"
        tickers_set.add(btc_ticker_name)
        
        final_tickers = sorted(list(tickers_set))

        if not final_tickers:
             raise ValueError("La lista ticker è vuota.")
        print(f"Trovati {len(final_tickers)} tickers unici (incluso BTC).")
        return final_tickers

    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile recuperare la lista dei ticker. {e}")
        raise

def fetch_full_history_for_ticker(ticker: str, api_key: str, start_date: str) -> Optional[pd.DataFrame]:
    # --- SOLUZIONE: Aggiunto &from={start_date} all'URL ---
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d&from={start_date}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data: return None
        
        df = pd.DataFrame(data)
        required_cols = ['date', 'adjusted_close', 'volume']
        df = df[required_cols]
        df = df.rename(columns={'adjusted_close': 'close'})
        return df

    except requests.exceptions.RequestException as e:
        print(f"  - ERRORE API durante il download di {ticker}: {e}")
        return None

if __name__ == "__main__":
    if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
        raise ValueError("Errore: una o più variabili d'ambiente necessarie non sono state impostate.")

    gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)
    
    root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
    if not root_folder_id: raise FileNotFoundError(f"'{ROOT_FOLDER_NAME}' non trovata.")

    raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
    if not raw_history_folder_id: raise FileNotFoundError(f"'{RAW_HISTORY_FOLDER_NAME}' non trovata.")

    all_tickers = get_all_tickers(EODHD_API_KEY, CRYPTO_EXCHANGE_CODE)
    total_tickers = len(all_tickers)
    
    print(f"\nInizio download e salvataggio di {total_tickers} file storici (dal {START_DATE})...")
    for i, ticker in enumerate(all_tickers):
        try:
            print(f"Processo {i+1}/{total_tickers}: {ticker}")
            # --- SOLUZIONE: Passa START_DATE alla funzione ---
            history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY, START_DATE)
            
            if history_df is not None and not history_df.empty:
                history_df['date'] = pd.to_datetime(history_df['date'])
                # Non serve impostare l'indice qui, lo facciamo all'upload
                file_name = f"{ticker}.parquet"
                upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            else:
                print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        except Exception as e:
            print(f"!!! FALLIMENTO IRRECUPERABILE per {ticker}: {e}. Continuo col prossimo.")
            continue
        
        time.sleep(0.2) 

    print("\n>>> Processo di REFRESH COMPLETO terminato.")
