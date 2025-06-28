# add_missing_ticker.py (VERSIONE CORRETTA E MIRATA)

import os
import pandas as pd
import requests
import time
import traceback
from typing import Optional
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
ROOT_FOLDER_NAME = "KriterionQuant_Data"
RAW_HISTORY_FOLDER_NAME = "raw-history"
START_DATE = "2018-01-01"

# --- TICKER DA FORZARE ---
TICKERS_TO_FIX = [
    "BTC-USD.CC"
]

def fetch_history_for_ticker(ticker: str, api_key: str, start_date: str) -> Optional[pd.DataFrame]:
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d&from={start_date}"
    print(f"Tentativo di download per {ticker}...")
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        if not data:
            print(f"  - L'API non ha restituito dati per {ticker}.")
            return None
        
        df = pd.DataFrame(data)

        final_data = {}
        if 'date' not in df.columns: return None
        final_data['date'] = df['date']
        
        if 'adjusted_close' in df.columns and df['adjusted_close'].notna().any():
            final_data['close'] = df['adjusted_close']
        elif 'close' in df.columns:
            final_data['close'] = df['close']
        else:
            return None

        if 'volume' in df.columns:
            final_data['volume'] = df['volume']
        else:
            return None
        
        print(f"  - Dati per {ticker} scaricati e colonne verificate.")
        return pd.DataFrame(final_data)

    except requests.exceptions.RequestException as e:
        print(f"  - ERRORE API durante il download di {ticker}: {e}")
        return None

if __name__ == "__main__":
    try:
        print(">>> Inizio processo di FIX MANUALE per ticker mancanti...")

        if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
            raise ValueError("Le variabili d'ambiente non sono impostate.")

        gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)

        print("Ricerca cartelle su Google Drive...")
        root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
        if not root_folder_id: raise FileNotFoundError(f"'{ROOT_FOLDER_NAME}' non trovata.")

        raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        if not raw_history_folder_id: raise FileNotFoundError(f"'{RAW_HISTORY_FOLDER_NAME}' non trovata.")
        print("Cartelle trovate con successo.")

        for ticker in TICKERS_TO_FIX:
            print(f"\nProcesso di fix per: {ticker}")
            history_df = fetch_history_for_ticker(ticker, EODHD_API_KEY, START_DATE)

            if history_df is not None and not history_df.empty:
                file_name = f"{ticker}.parquet"
                print(f"Dati validi trovati. Avvio upload su Google Drive come '{file_name}'...")
                upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            else:
                raise ValueError(f"Download fallito per {ticker}. Dati non disponibili o vuoti.")
        
        print("\n>>> Processo di FIX MANUALE terminato con SUCCESSO.")

    except Exception as e:
        print(f"!!! PROCESSO DI FIX MANUALE FALLITO: {e}")
        traceback.print_exc()
        raise
