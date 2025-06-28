# run_full_refresh.py (CORRETTO E ROBUSTO)

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
START_DATE = "2018-01-01"

def get_all_tickers(api_key: str, exchange_code: str) -> List[str]:
    """
    Recupera tutti i ticker, gestendo diversi formati e garantendo la presenza di BTC-USD.CC.
    Questa logica è più robusta e fedele all'idea originale.
    """
    print(f"Recupero lista ticker per exchange '{exchange_code}'...")
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        
        all_tickers = set()
        btc_ticker_name = "BTC-USD.CC"
        
        for item in data:
            code = item.get("Code")
            if not code or not isinstance(code, str):
                continue

            # EODHD a volte restituisce solo il simbolo base (es. "ETH") 
            # e altre volte il pair completo (es. "ETH-USD")
            if code.endswith('-USD'):
                all_tickers.add(f"{code}.{exchange_code}")
            else:
                # Se non contiene un trattino, è un simbolo base (es. "XRP"). Costruiamo il pair.
                # Questo esclude pair non-USD come "ETH-BTC".
                if '-' not in code:
                     all_tickers.add(f"{code}-USD.{exchange_code}")

        # LOGICA DI SICUREZZA RIPRISTINATA: Assicura che Bitcoin sia SEMPRE presente.
        all_tickers.add(btc_ticker_name)

        final_tickers = sorted(list(all_tickers))

        if not final_tickers:
             raise ValueError("La lista ticker è vuota anche dopo l'aggiunta forzata di BTC.")
        print(f"Trovati {len(final_tickers)} tickers unici (incluso BTC garantito).")
        return final_tickers

    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile recuperare la lista dei ticker. {e}")
        raise

def fetch_full_history_for_ticker(ticker: str, api_key: str, start_date: str) -> Optional[pd.DataFrame]:
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d&from={start_date}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data: return None
        
        df = pd.DataFrame(data)
        required_cols = ['date', 'adjusted_close', 'volume']
        
        if not all(col in df.columns for col in required_cols):
            print(f"  - Dati incompleti per {ticker} (colonne mancanti). Salto.")
            return None
            
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
            history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY, START_DATE)
            
            if history_df is not None and not history_df.empty:
                file_name = f"{ticker}.parquet"
                upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            else:
                print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        except Exception as e:
            print(f"!!! FALLIMENTO per {ticker}: {e}. Continuo col prossimo.")
            continue
        
        time.sleep(0.2) 

    print("\n>>> Processo di REFRESH COMPLETO terminato.")
