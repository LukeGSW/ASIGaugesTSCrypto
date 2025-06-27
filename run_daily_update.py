# run_daily_update.py

import os
import pandas as pd
import numpy as np
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
        raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        production_folder_id = find_id(gdrive_service, name=PRODUCTION_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        print("Cartelle trovate con successo.")

        # --- FASE 1: CARICAMENTO DATI STORICI ---
        raw_history_df = download_all_parquets_in_folder(gdrive_service, raw_history_folder_id)
        
        # --- FASE 2: CARICAMENTO DATI INCREMENTALI ---
        tickers_list = raw_history_df['ticker'].unique().tolist()
        daily_delta_df = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        # --- FASE 3: RICOSTRUZIONE DEL DATAFRAME (LA SOLUZIONE DEFINITIVA) ---
        if daily_delta_df is not None and not daily_delta_df.empty:
            print("Dati incrementali trovati. Ricostruisco il DataFrame finale...")
            
            # Assicuriamo che entrambi i DataFrame abbiano le stesse colonne nello stesso ordine
            final_cols = ['date', 'close', 'volume', 'ticker']
            raw_history_df = raw_history_df[final_cols]
            daily_delta_df = daily_delta_df[final_cols]

            # Uniamo i due DataFrame e rimuoviamo i duplicati basati su data e ticker, tenendo l'ultimo valore
            combined_df = pd.concat([raw_history_df, daily_delta_df], ignore_index=True)
            updated_history_df = combined_df.drop_duplicates(subset=['date', 'ticker'], keep='last').sort_values('date').reset_index(drop=True)
            
            print("Ricostruzione e pulizia finale completati.")
        else:
            print("Nessun nuovo dato dall'API. Eseguo solo pulizia su dati storici.")
            updated_history_df = raw_history_df.drop_duplicates(subset=['date', 'ticker'], keep='last').sort_values('date').reset_index(drop=True)

        print(f"DataFrame finale pronto per l'analisi con {len(updated_history_df)} righe uniche.")
        
        # --- FASE 4: CALCOLO ---
        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = dp.create_dynamic_baskets(updated_history_df)
        print(f"Generati {len(dynamic_baskets)} panieri.")

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = dp.calculate_full_asi(updated_history_df)
        
        if final_asi_df.empty:
             print("!!! ATTENZIONE: Il DataFrame finale dell'ASI è VUOTO.")
        
        print(f"Calcolo ASI completato. Il DataFrame finale ha {len(final_asi_df)} righe.")
        
        # --- FASE 5: UPLOAD ---
        upload_or_update_parquet(gdrive_service, final_asi_df, PRODUCTION_FILE_NAME, production_folder_id)

        print("\n>>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        import traceback
        traceback.print_exc()
        raise
