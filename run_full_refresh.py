# run_full_refresh.py
# Eseguito manualmente per ricaricare da zero l'intera base dati storica su Google Drive.

import os
import pandas as pd
import requests
import time
from typing import List, Dict, Optional

# Importiamo il nostro nuovo modulo di servizio per Google Drive
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
CRYPTO_EXCHANGE_CODE = "CC" # Codice per crypto, es. CryptoCompare
ROOT_FOLDER_NAME = "KriterionQuant_Data"
RAW_HISTORY_FOLDER_NAME = "raw-history"

# --- FUNZIONI HELPER PER EODHD (le manteniamo qui per specificità dello script) ---

def get_all_tickers(api_key: str, exchange_code: str) -> List[str]:
    """Recupera la lista di tutti i ticker per un dato exchange da EODHD."""
    print(f"Recupero lista ticker per exchange '{exchange_code}'...")
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        # Filtriamo per assicurarci di prendere solo i ticker desiderati
        tickers = [
            item['Code'] + '.' + exchange_code 
            for item in data 
            if item.get('Type') in ['Common Stock', 'CRYPTO'] # 'Common Stock' è usato anche per le crypto
        ]
        print(f"Trovati {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile recuperare la lista dei ticker. {e}")
        raise

def fetch_full_history_for_ticker(ticker: str, api_key: str) -> Optional[pd.DataFrame]:
    """Scarica la storia completa per un singolo ticker."""
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"  - ERRORE API durante il download di {ticker}: {e}")
        return None

# --- BLOCCO DI ESECUZIONE PRINCIPALE ---

if __name__ == "__main__":
    print(">>> Inizio processo di REFRESH COMPLETO dei dati storici su Google Drive...")
    
    if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
        raise ValueError("Errore: mancano le variabili d'ambiente EODHD_API_KEY o GDRIVE_SA_KEY.")

    # 1. Autenticazione e setup dei servizi
    gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)
    
    # 2. Trova le cartelle necessarie su Google Drive
    print("Ricerca cartelle su Google Drive...")
    root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
    if not root_folder_id:
        raise FileNotFoundError(f"La cartella radice '{ROOT_FOLDER_NAME}' non è stata trovata su Google Drive.")
        
    raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
    if not raw_history_folder_id:
        raise FileNotFoundError(f"La sottocartella '{RAW_HISTORY_FOLDER_NAME}' non è stata trovata.")
    print("Cartelle trovate con successo.")

    # 3. Ottieni la lista completa dei ticker da processare
    all_tickers = get_all_tickers(EODHD_API_KEY, CRYPTO_EXCHANGE_CODE)
    total_tickers = len(all_tickers)
    
    # 4. Itera, scarica e salva ogni ticker su Google Drive
    print(f"\nInizio download e salvataggio di {total_tickers} file storici...")
    for i, ticker in enumerate(all_tickers):
        print(f"Processo {i+1}/{total_tickers}: {ticker}")
        
        history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY)
        
        if history_df is not None and not history_df.empty:
            # Salva il file .parquet nella cartella `raw-history/` su Drive
            file_name = f"{ticker}.parquet"
            upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
        else:
            print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        
        # Pausa per rispettare i limiti dell'API
        time.sleep(0.2) 

    print("\n>>> Processo di REFRESH COMPLETO terminato.")
