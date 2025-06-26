# run_daily_update.py (VERSIONE DI DEBUG)

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
        
        # --- BLOCCO DI DEBUG #1: Analisi del primo DataFrame ---
        print("\n--- DEBUG: Analisi di 'raw_history_df' ---")
        print(f"Shape: {raw_history_df.shape}")
        print(f"Indice è unico? {raw_history_df.index.is_unique}")
        print(f"Numero di duplicati nell'indice: {raw_history_df.index.duplicated().sum()}")
        print("Prime 5 righe dell'indice:", raw_history_df.index[:5])
        print("--------------------------------------------\n")
        
        # 2. Scarica il "delta" incrementale dall'API di EODHD
        tickers_list = raw_history_df['ticker'].unique().tolist()
        daily_delta_df = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        if daily_delta_df is not None and not daily_delta_df.empty:
            # --- BLOCCO DI DEBUG #2: Analisi del secondo DataFrame ---
            print("\n--- DEBUG: Analisi di 'daily_delta_df' ---")
            print(f"Shape: {daily_delta_df.shape}")
            print(f"Indice è unico? {daily_delta_df.index.is_unique}")
            print(f"Numero di duplicati nell'indice: {daily_delta_df.index.duplicated().sum()}")
            print("Prime 5 righe dell'indice:", daily_delta_df.index[:5])
            print("-------------------------------------------\n")

            # 3. Unisci i dati
            try:
                print(">>> Tentativo di unire i dati con pd.concat(ignore_index=True)...")
                updated_history_df = pd.concat(
                    [raw_history_df, daily_delta_df], 
                    ignore_index=True
                ).drop_duplicates(subset=['date', 'ticker'], keep='last').sort_values('date')
                print(">>> Unione dati completata con successo.")
            except Exception as e:
                print("\n!!! ERRORE INTERCETTATO DURANTE pd.concat !!!")
                print(f"Tipo di errore: {type(e)}")
                print(f"Messaggio di errore: {e}")
                raise e # Rilanciamo l'errore per far fallire il job
        else:
            print("Nessun nuovo dato dall'API, procedo con i dati esistenti.")
            updated_history_df = raw_history_df

        # 4. Esegui la logica di calcolo principale
        # ... (il resto dello script rimane invariato)
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
