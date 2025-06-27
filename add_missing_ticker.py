# add_missing_ticker.py

import os
import time
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet
from run_full_refresh import fetch_full_history_for_ticker

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
ROOT_FOLDER_NAME = "KriterionQuant_Data"
RAW_HISTORY_FOLDER_NAME = "raw-history"

# --- LISTA DEI TICKER DA CORREGGERE/AGGIUNGERE ---
TICKERS_TO_FIX = [
    "BTC-USD.CC"
]

if __name__ == "__main__":
    print(">>> Inizio processo di FIX MANUALE per ticker mancanti...")

    if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
        raise ValueError("Errore: una o più variabili d'ambiente necessarie non sono state impostate.")

    gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)

    print("Ricerca cartelle su Google Drive...")
    root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
    if not root_folder_id: raise FileNotFoundError(f"'{ROOT_FOLDER_NAME}' non trovata.")

    raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
    if not raw_history_folder_id: raise FileNotFoundError(f"'{RAW_HISTORY_FOLDER_NAME}' non trovata.")
    print("Cartelle trovate con successo.")

    print(f"\nInizio download e salvataggio di {len(TICKERS_TO_FIX)} ticker specifici...")
    for ticker in TICKERS_TO_FIX:
        try:
            print(f"Processo: {ticker}")
            history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY)

            if history_df is not None and not history_df.empty:
                file_name = f"{ticker}.parquet"
                upload_or_update_parquet(gdrive_service, history_df, file_name, raw_history_folder_id)
            else:
                print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        except Exception as e:
            print(f"!!! FALLIMENTO IRRECUPERABILE per {ticker}: {e}.")
            # A differenza dello script principale, qui vogliamo che il job fallisca se l'unico ticker non funziona
            raise

        time.sleep(0.2) 

    print("\n>>> Processo di FIX MANUALE terminato con successo.")
