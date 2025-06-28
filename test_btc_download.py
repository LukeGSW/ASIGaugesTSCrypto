# test_btc_download.py

import os
import pandas as pd
import requests
import traceback
from typing import Optional
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
ROOT_FOLDER_NAME = "KriterionQuant_Data"
RAW_HISTORY_FOLDER_NAME = "raw-history"
START_DATE = "2018-01-01"
TICKER_TO_TEST = "BTC-USD.CC"

def fetch_history_for_single_ticker(ticker: str, api_key: str, start_date: str) -> Optional[pd.DataFrame]:
    """
    Questa è la versione corretta della funzione di download.
    La testiamo qui in isolamento.
    """
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d&from={start_date}"
    print(f"Tentativo di download per {ticker} dall'URL: {url.split('?')[0]}...")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            print(f"  - L'API non ha restituito dati per {ticker}.")
            return None
        
        df = pd.DataFrame(data)

        # Logica robusta per gestire 'adjusted_close' vs 'close'
        if 'adjusted_close' in df.columns and df['adjusted_close'].notna().any():
            df = df.rename(columns={'adjusted_close': 'close'})
        elif 'close' not in df.columns:
            print(f"  - Dati per {ticker} non contengono né 'close' né 'adjusted_close'.")
            return None
        
        required_cols = ['date', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            print(f"  - Dati incompleti per {ticker}. Colonne presenti: {df.columns.to_list()}.")
            return None

        print(f"  - Dati per {ticker} scaricati con successo. Colonne: {df[required_cols].columns.to_list()}")
        return df[required_cols]

    except requests.exceptions.RequestException as e:
        print(f"  - ERRORE API durante il download di {ticker}: {e}")
        return None

if __name__ == "__main__":
    try:
        print(">>> INIZIO TEST RAPIDO PER IL DOWNLOAD DI BTC <<<")
        if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
            raise ValueError("Le variabili d'ambiente non sono impostate.")

        gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)
        
        root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
        raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')

        print(f"Processo di test per: {TICKER_TO_TEST}")
        history_df = fetch_history_for_single_ticker(TICKER_TO_TEST, EODHD_API_KEY, START_DATE)
        
        if history_df is not None and not history_df.empty:
            file_name = f"{TICKER_TO_TEST}.parquet"
            print(f"Dati di BTC scaricati, avvio upload su Google Drive come '{file_name}'...")
            upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            print(">>> TEST RAPIDO COMPLETATO CON SUCCESSO! <<<")
        else:
            raise ValueError(f"Download fallito per {TICKER_TO_TEST}. Dati non disponibili o vuoti.")

    except Exception as e:
        print(f"!!! TEST RAPIDO FALLITO: {e}")
        traceback.print_exc()
        raise
