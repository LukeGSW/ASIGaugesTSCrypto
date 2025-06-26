# run_full_refresh.py

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

def get_all_tickers(api_key: str, exchange_code: str) -> List[str]:
    print(f"Recupero lista ticker per exchange '{exchange_code}'...")
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        tickers = [
            item['Code'] + '.' + exchange_code 
            for item in data 
            if item.get('Code', '').endswith('-USD')
        ]
        if not tickers:
             raise ValueError("La lista ticker restituita da EODHD è vuota dopo il filtro.")
        print(f"Trovati {len(tickers)} tickers che terminano in -USD.")
        return tickers
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile recuperare la lista dei ticker. {e}")
        raise

def fetch_full_history_for_ticker(ticker: str, api_key: str) -> Optional[pd.DataFrame]:
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        
        df = pd.DataFrame(data)
        # --- MODIFICA CHIAVE QUI ---
        # Selezioniamo SOLO le colonne che ci servono, scartando il resto.
        required_cols = ['date', 'adjusted_close', 'volume']
        df = df[required_cols]
        df = df.rename(columns={'adjusted_close': 'close'}) # Rinominiamo per coerenza
        return df

    except requests.exceptions.RequestException as e:
        print(f"  - ERRORE API durante il download di {ticker}: {e}")
        return None

if __name__ == "__main__":
    print(">>> Inizio processo di REFRESH COMPLETO dei dati storici su Google Drive...")
    
    if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
        raise ValueError("Errore: mancano le variabili d'ambiente.")

    gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)
    
    print("Ricerca cartelle su Google Drive...")
    root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
    if not root_folder_id: raise FileNotFoundError(f"'{ROOT_FOLDER_NAME}' non trovata.")
        
    raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
    if not raw_history_folder_id: raise FileNotFoundError(f"'{RAW_HISTORY_FOLDER_NAME}' non trovata.")
    print("Cartelle trovate con successo.")

    all_tickers = get_all_tickers(EODHD_API_KEY, CRYPTO_EXCHANGE_CODE)
    total_tickers = len(all_tickers)
    
    print(f"\nInizio download e salvataggio di {total_tickers} file storici...")
    for i, ticker in enumerate(all_tickers):
        try:
            print(f"Processo {i+1}/{total_tickers}: {ticker}")
            history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY)
            
            if history_df is not None and not history_df.empty:
                file_name = f"{ticker}.parquet"
                upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            else:
                print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        except Exception as e:
            print(f"!!! FALLIMENTO IRRECUPERABILE per {ticker}: {e}. Continuo col prossimo.")
            continue
        
        time.sleep(0.2) 

    print("\n>>> Processo di REFRESH COMPLETO terminato.")
