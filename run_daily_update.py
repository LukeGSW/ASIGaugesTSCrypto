# run_daily_update.py (VERSIONE CON DEBUG AVANZATO)

import os
import pandas as pd
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet, download_all_parquets_in_folder
import src.data_processing as dp

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
ROOT_FOLDER_NAME = "KriterionQuant_Data"
RAW_HISTORY_FOLDER_NAME = "raw-history"
PRODUCTION_FOLDER_NAME = "production"
PRODUCTION_FILE_NAME = "altcoin_season_index.parquet"

# --- BLOCCO DI ESECUZIONE PRINCIPALE ---
if __name__ == "__main__":
    print(">>> Inizio processo di aggiornamento quotidiano dell'ASI (Google Drive)...")

    if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
        raise ValueError("Errore: una o più variabili d'ambiente necessarie non sono state impostate.")

    gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)
    
    try:
        print("Ricerca cartelle su Google Drive...")
        root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
        if not root_folder_id: raise FileNotFoundError(f"'{ROOT_FOLDER_NAME}' non trovata.")
            
        raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        if not raw_history_folder_id: raise FileNotFoundError(f"'{RAW_HISTORY_FOLDER_NAME}' non trovata.")
            
        production_folder_id = find_id(gdrive_service, name=PRODUCTION_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        if not production_folder_id: raise FileNotFoundError(f"'{PRODUCTION_FOLDER_NAME}' non trovata.")
        print("Cartelle trovate con successo.")

        # 1. Carica la base dati storica completa da Google Drive
        raw_history_df = download_all_parquets_in_folder(gdrive_service, raw_history_folder_id)
        
        # --- NUOVO BLOCCO DI DEBUG: VERIFICA PRESENZA BTC ---
        print("\n--- DEBUG: VERIFICA PRESENZA BTC IN 'raw_history_df' ---")
        btc_ticker_name = "BTC-USD.CC"
        # Controlliamo se la stringa esatta del ticker è presente nella colonna 'ticker'
        is_btc_present = btc_ticker_name in raw_history_df['ticker'].unique()
        print(f"Il ticker '{btc_ticker_name}' è presente nel DataFrame caricato da GDrive? -> {is_btc_present}")
        if not is_btc_present:
            print("Elenco dei primi 20 ticker UNICI trovati nel DataFrame (potrebbe aiutare a identificare problemi di nomi):")
            print(raw_history_df['ticker'].unique()[:20])
        print("----------------------------------------------------------\n")
        # --- FINE NUOVO BLOCCO DI DEBUG ---

        # 2. Scarica il "delta" incrementale dall'API di EODHD
        tickers_list = raw_history_df['ticker'].unique().tolist()
        daily_delta_df = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        if daily_delta_df is not None and not daily_delta_df.empty:
            # 3. Unisci i dati
            updated_history_df = pd.concat(
                [raw_history_df, daily_delta_df], 
                ignore_index=True
            ).drop_duplicates(subset=['date', 'ticker'], keep='last').sort_values('date')
            print("Unione dati (storico + delta) completata.")
        else:
            print("Nessun nuovo dato dall'API, procedo con i dati storici esistenti.")
            updated_history_df = raw_history_df

        # 4. Esegui la logica di calcolo principale
        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = dp.create_dynamic_baskets(updated_history_df)
        print(f"Generati {len(dynamic_baskets)} panieri.")

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = dp.calculate_full_asi(updated_history_df, dynamic_baskets)
        print("Calcolo ASI completato.")

        upload_or_update_parquet(gdrive_service, final_asi_df, PRODUCTION_FILE_NAME, production_folder_id)

        print("\n>>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        raise
