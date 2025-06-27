# run_daily_update.py

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
        raw_history_folder_id = find_id(gdrive_service, name=RAW_HISTORY_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        production_folder_id = find_id(gdrive_service, name=PRODUCTION_FOLDER_NAME, parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        print("Cartelle trovate con successo.")

        # --- FASE 1: CARICAMENTO E PULIZIA DATI STORICI ---
        raw_history_df = download_all_parquets_in_folder(gdrive_service, raw_history_folder_id)
        # PULIZIA PREVENTIVA: Rimuoviamo duplicati che potrebbero essere già presenti nei file storici
        raw_history_df.drop_duplicates(subset=['date', 'ticker'], keep='last', inplace=True)
        print(f"Dati storici caricati e puliti: {len(raw_history_df)} righe.")

        # --- FASE 2: CARICAMENTO E PULIZIA DATI INCREMENTALI ---
        tickers_list = raw_history_df['ticker'].unique().tolist()
        daily_delta_df = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        # --- FASE 3: UNIONE E PULIZIA FINALE ---
        if daily_delta_df is not None and not daily_delta_df.empty:
            print("Dati incrementali trovati. Eseguo unione e pulizia finale...")
            # Pulisci anche il delta prima di unirlo, per massima sicurezza
            daily_delta_df.drop_duplicates(subset=['date', 'ticker'], keep='last', inplace=True)
            
            # Usiamo ignore_index=True per creare un indice pulito e sequenziale dopo l'unione
            updated_history_df = pd.concat(
                [raw_history_df, daily_delta_df], 
                ignore_index=True 
            )
            # Un'ultima pulizia sul dataframe combinato per eliminare ogni possibile sovrapposizione
            updated_history_df.drop_duplicates(subset=['date', 'ticker'], keep='last', inplace=True)
            updated_history_df.sort_values('date', inplace=True)
            
            print("Unione e pulizia finale completati.")
        else:
            print("Nessun nuovo dato dall'API, procedo con i dati storici già puliti.")
            updated_history_df = raw_history_df

        print(f"DataFrame finale pronto per l'analisi con {len(updated_history_df)} righe uniche.")
        
        # --- FASE 4: CALCOLO ---
        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = dp.create_dynamic_baskets(updated_history_df, performance_window=90)
        print(f"Generati {len(dynamic_baskets)} panieri.")

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = dp.calculate_full_asi(updated_history_df, dynamic_baskets)
        
        if final_asi_df.empty:
             print("ATTENZIONE: Il DataFrame finale dell'ASI è VUOTO.")
        
        print(f"Calcolo ASI completato. Il DataFrame finale ha {len(final_asi_df)} righe.")
        
        # --- FASE 5: UPLOAD ---
        upload_or_update_parquet(gdrive_service, final_asi_df, PRODUCTION_FILE_NAME, production_folder_id)

        print("\n>>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        import traceback
        traceback.print_exc()
        raise
