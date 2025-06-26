# run_full_refresh.py
# Eseguito manualmente per ricaricare da zero l'intera base dati storica su Google Drive.

import os
import io
import pandas as pd
import requests
import time
from typing import List, Optional

# Importiamo il nostro nuovo modulo di servizio per Google Drive
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
CRYPTO_EXCHANGE_CODE = "CC" 
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
        
        # --- MODIFICA CHIAVE QUI ---
        # Filtriamo per i ticker che terminano in "-USD" invece che per il "Tipo".
        # Questo è un criterio molto più affidabile per le coppie crypto.
        tickers = [
            item['Code'] + '.' + exchange_code 
            for item in data 
            if item.get('Code', '').endswith('-USD')
        ]
        
        if not tickers:
             raise ValueError("La lista ticker restituita da EODHD è vuota dopo il filtro. Controllare l'API o il filtro.")

        print(f"Trovati {len(tickers)} tickers che terminano in -USD.")
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

# Il resto del file (blocco di esecuzione principale) rimane identico...
# ...
if __name__ == "__main__":
    # ... (l'autenticazione e la ricerca delle cartelle rimangono identiche) ...

    all_tickers = get_all_tickers(EODHD_API_KEY, CRYPTO_EXCHANGE_CODE)
    total_tickers = len(all_tickers)
    
    print(f"\nInizio download e salvataggio di {total_tickers} file storici...")
    for i, ticker in enumerate(all_tickers):
        try: # --- BLOCCO TRY/EXCEPT AGGIUNTO ---
            print(f"Processo {i+1}/{total_tickers}: {ticker}")
            
            history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY)
            
            if history_df is not None and not history_df.empty:
                file_name = f"{ticker}.parquet"
                upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            else:
                print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        except Exception as e:
            # Se qualcosa va storto per un ticker dopo tutti i tentativi, lo logga e va avanti
            print(f"!!! FALLIMENTO IRRECUPERABILE per il ticker {ticker}: {e}. Continuo con il prossimo.")
            continue
        
        time.sleep(0.2) 

    print("\n>>> Processo di REFRESH COMPLETO terminato.")
