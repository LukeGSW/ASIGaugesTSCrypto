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

        historical_data_dict = download_all_parquets_in_folder(gdrive_service, raw_history_folder_id)
        print(f"Dati storici caricati per {len(historical_data_dict)} tickers.")

        tickers_list = list(historical_data_dict.keys())
        daily_delta_dict = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        # Sostituisci il blocco if/for esistente con questo
        if daily_delta_dict:
    print("Dati incrementali trovati. Eseguo unione nel dizionario...")
    for ticker, delta_df in daily_delta_dict.items():
        if ticker in historical_data_dict:
            hist_df = historical_data_dict[ticker]
            
            # --- INIZIO DELLA SOLUZIONE ROBUSTA ---
            
            # 1. ASSICURA L'UNICITÀ DELLO STORICO: Questo è il passo chiave mancante.
            #    Controlla se l'indice del DataFrame storico ha duplicati e, in caso, li rimuove.
            if not hist_df.index.is_unique:
                print(f"  - ATTENZIONE: Trovato e rimosso indice duplicato nello storico per {ticker}.")
                hist_df = hist_df[~hist_df.index.duplicated(keep='last')]

            # 2. ASSICURA L'UNICITÀ DEL DELTA (doppia sicurezza, anche se improbabile dall'API).
            if not delta_df.index.is_unique:
                delta_df = delta_df[~delta_df.index.duplicated(keep='last')]
            
            # 3. Rimuovi dallo storico (ora pulito) le righe le cui date sono presenti nel nuovo delta.
            hist_df_cleaned = hist_df.drop(delta_df.index, errors='ignore')
            
            # 4. Concatena in sicurezza. Ora entrambi i DataFrame hanno indici unici e non sovrapposti.
            combined_df = pd.concat([hist_df_cleaned, delta_df])
            
            # 5. Ordina l'indice per mantenere la cronologia corretta.
            combined_df.sort_index(inplace=True)
            
            historical_data_dict[ticker] = combined_df
            # --- FINE DELLA SOLUZIONE ROBUSTA ---

        else:
            # Se il ticker è nuovo, aggiungilo semplicemente.
            historical_data_dict[ticker] = delta_df
            
    print("Unione nel dizionario completata.")
else:
    print("Nessun nuovo dato dall'API.")

        full_df_for_baskets = pd.concat(historical_data_dict.values()).reset_index()
        
        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = dp.create_dynamic_baskets(full_df_for_baskets)
        print(f"Generati {len(dynamic_baskets)} panieri.")

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = dp.calculate_full_asi(historical_data_dict, dynamic_baskets)
        
        if final_asi_df.empty:
             print("ATTENZIONE: Il DataFrame finale dell'ASI è VUOTO.")
        
        print(f"Calcolo ASI completato. Il DataFrame finale ha {len(final_asi_df)} righe.")
        
        upload_or_update_parquet(gdrive_service, final_asi_df, PRODUCTION_FILE_NAME, production_folder_id)

        print("\n>>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        import traceback
        traceback.print_exc()
        raise
