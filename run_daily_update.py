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

        raw_history_df = download_all_parquets_in_folder(gdrive_service, raw_history_folder_id)
        tickers_list = raw_history_df['ticker'].unique().tolist()
        daily_delta_df = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        if daily_delta_df is not None and not daily_delta_df.empty:
            updated_history_df = pd.concat(
                [raw_history_df, daily_delta_df], 
                ignore_index=True
            ).drop_duplicates(subset=['date', 'ticker'], keep='last').sort_values('date')
            print("Unione dati (storico + delta) completata.")
        else:
            print("Nessun nuovo dato dall'API, procedo con i dati storici esistenti.")
            updated_history_df = raw_history_df
        
        # --- BLOCCO DI ISPEZIONE FINALE DEI DATI ---
        print("\n\n--- ISPEZIONE FINALE DEL DATAFRAME 'updated_history_df' ---")
        print("Questo è un riassunto dei dati ESATTI che stanno per essere usati per creare i panieri.")
        
        print("\n[1] Informazioni Generali (Tipi di dato e valori non nulli):")
        updated_history_df.info(verbose=True, show_counts=True)
        
        print("\n[2] Ultime 10 righe del DataFrame:")
        print(updated_history_df.tail(10))

        print("\n[3] Statistiche Descrittive per le colonne numeriche:")
        try:
            print(updated_history_df.describe())
        except Exception as desc_e:
            print(f"Impossibile generare statistiche descrittive: {desc_e}")

        print("\n[4] Controllo dei volumi per gli ultimi 90 giorni:")
        try:
            last_90_days_df = updated_history_df[updated_history_df['date'] > (updated_history_df['date'].max() - pd.Timedelta(days=90))]
            volume_summary = last_90_days_df.groupby('ticker')['volume'].sum().sort_values(ascending=False)
            print("Primi 20 ticker per volume negli ultimi 90 giorni:")
            print(volume_summary.head(20))
            print("...\nUltimi 20 ticker per volume negli ultimi 90 giorni:")
            print(volume_summary.tail(20))
            print(f"\nVolume totale per BTC negli ultimi 90 giorni: {volume_summary.get('BTC-USD.CC', 'Non trovato')}")
        except Exception as vol_e:
            print(f"Impossibile generare riassunto volumi: {vol_e}")
            
        print("--- FINE ISPEZIONE ---\n\n")
        # --- FINE BLOCCO DI ISPEZIONE ---

        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = dp.create_dynamic_baskets(updated_history_df)

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = dp.calculate_full_asi(updated_history_df, dynamic_baskets)
        
        if final_asi_df.empty:
            print("!!! ATTENZIONE FINALE: Il DataFrame calcolato è VUOTO. Verrà salvato un file vuoto. !!!")

        upload_or_update_parquet(gdrive_service, final_asi_df, PRODUCTION_FILE_NAME, production_folder_id)

        print("\n>>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        raise
