import os
import pandas as pd
import traceback
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

        if daily_delta_dict:
            print("Dati incrementali trovati. Eseguo unione nel dizionario...")
            for ticker, delta_df in daily_delta_dict.items():
                if ticker in historical_data_dict:
                    hist_df = historical_data_dict[ticker]
                    
                    # --- INIZIO SOLUZIONE ROBUSTA (APPROCCIO RESET INDEX) ---
                    
                    # 1. Resetta l'indice di entrambi i DataFrame, 'date' diventa una colonna.
                    hist_df_reset = hist_df.reset_index()
                    delta_df_reset = delta_df.reset_index()
                    
                    # 2. Concatena usando l'indice numerico di default. Questo non può fallire.
                    combined_df = pd.concat([hist_df_reset, delta_df_reset], ignore_index=True)
                    
                    # 3. Rimuovi i duplicati basandoti sulla colonna 'date', tenendo l'ultimo valore (dal delta).
                    combined_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
                    
                    # 4. Reimposta 'date' come indice e ordina.
                    combined_df.set_index('date', inplace=True)
                    combined_df.sort_index(inplace=True)
                    
                    historical_data_dict[ticker] = combined_df
                    # --- FINE SOLUZIONE ROBUSTA ---

                else:
                    historical_data_dict[ticker] = delta_df
            print("Unione nel dizionario completata.")
        else:
            print("Nessun nuovo dato dall'API.")
        
        # --- BLOCCO SUCCESSIVO (INVARIATO) ---
        
        # Aggiungo un ticker_map per associare correttamente i ticker ai dati
        ticker_map = {i: ticker for i, ticker in enumerate(historical_data_dict.keys())}
        # Prepara una lista di dataframe dove ogni df ha la colonna ticker
        df_list_for_concat = []
        for ticker, df in historical_data_dict.items():
            df_copy = df.copy()
            df_copy['ticker'] = ticker
            df_list_for_concat.append(df_copy)

        full_df_for_baskets = pd.concat(df_list_for_concat).reset_index()
        
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
        traceback.print_exc()
        raise
