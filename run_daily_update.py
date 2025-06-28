import os
import pandas as pd
import traceback
import requests
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet
import src.data_processing as dp
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaIoBaseDownload
import io

EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
ROOT_FOLDER_NAME = "KriterionQuant_Data"
PRODUCTION_FOLDER_NAME = "production"
PRODUCTION_FILE_NAME = "altcoin_season_index.parquet"
HIST_FILES_FOLDER_ID = "1_WEblq4NIxkduVaPraiFUd9AniVKV0vJ"  # Replace with actual ID

def download_all_csv_in_folder(service: Resource, folder_id: str) -> dict[str, pd.DataFrame]:
    print(f"Ricerca file CSV nella cartella con ID: {folder_id}...")
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and name contains '.csv'"
    
    try:
        request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
        response = request.execute()
        files = response.get('files', [])
        
        if not files:
            raise FileNotFoundError(f"Nessun file CSV trovato nella cartella con ID '{folder_id}'.")
        
        data_dict = {}
        total_files = len(files)
        print(f"Trovati {total_files} file CSV. Inizio download...")
        
        for i, file in enumerate(files, 1):
            try:
                file_id = file.get('id')
                file_name = file.get('name')
                print(f"Download {i}/{total_files}: {file_name}")
                
                # Download the file
                request = service.files().get_media(fileId=file_id)
                file_buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(file_buffer, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                
                file_buffer.seek(0)
                df = pd.read_csv(file_buffer)
                
                # Verifica e formattazione
                if 'date' not in df.columns:
                    print(f"  - Errore: '{file_name}' non ha la colonna 'date'. Salto.")
                    continue
                
                # Converti le date in tz-naive
                df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
                df.set_index('date', inplace=True)
                df = df[['open', 'high', 'low', 'close', 'adjusted_close', 'volume']].dropna(subset=['close', 'volume'])
                df = df[~df.index.duplicated(keep='last')].sort_index()
                
                # Estrai il ticker dal nome del file (es. "BTC-USD.CC.csv" -> "BTC-USD.CC")
                ticker = file_name.replace('.csv', '')
                data_dict[ticker] = df
                
            except Exception as e:
                print(f"  - Errore durante il download di '{file_name}': {e}")
                continue
        
        if not data_dict:
            raise ValueError("Nessun dato valido scaricato dalla cartella con ID '{folder_id}'.")
        
        print(f"Dati storici caricati per {len(data_dict)} tickers.")
        return data_dict
    
    except Exception as e:
        print(f"Errore durante il download dei file CSV: {e}")
        raise

if __name__ == "__main__":
    print(f">>> Inizio processo di aggiornamento quotidiano dell'ASI (Google Drive) at {pd.Timestamp.now(tz='Europe/Paris').strftime('%Y-%m-%d %H:%M:%S %Z')}...")

    if not all([EODHD_API_KEY, GDRIVE_SA_KEY]):
        raise ValueError("Errore: una o più variabili d'ambiente necessarie non sono state impostate.")

    gdrive_service = get_gdrive_service(GDRIVE_SA_KEY)
    
    try:
        print("Ricerca cartelle su Google Drive...")
        
        # Verifica l'ID della cartella HistFiles
        if not HIST_FILES_FOLDER_ID:
            raise ValueError("Errore: HIST_FILES_FOLDER_ID non specificato. Inserire l'ID della cartella HistFiles.")
        
        # Debug: verifica se la cartella HistFiles è accessibile
        folder_info = gdrive_service.files().get(fileId=HIST_FILES_FOLDER_ID, fields='id, name').execute()
        print(f"Cartella HistFiles trovata: {folder_info.get('name')} (ID: {folder_info.get('id')})")

        # Trova l'ID della cartella di produzione
        root_folder_id = find_id(gdrive_service, name=ROOT_FOLDER_NAME, mime_type='application/vnd.google-apps.folder')
        if not root_folder_id:
            raise FileNotFoundError(f"Cartella radice '{ROOT_FOLDER_NAME}' non trovata.")
        
        production_folder_id = find_id(
            gdrive_service,
            name=PRODUCTION_FOLDER_NAME,
            parent_id=root_folder_id,
            mime_type='application/vnd.google-apps.folder'
        )
        if not production_folder_id:
            raise FileNotFoundError(f"Cartella '{PRODUCTION_FOLDER_NAME}' non trovata.")
        
        print(f"Cartelle trovate: HistFiles (ID: {HIST_FILES_FOLDER_ID}), Production (ID: {production_folder_id})")

        # Scarica i dati storici dalla cartella HistFiles
        historical_data_dict = download_all_csv_in_folder(gdrive_service, HIST_FILES_FOLDER_ID)
        
        # Aggiorna con i dati giornalieri dall'API
        tickers_list = list(historical_data_dict.keys())
        daily_delta_dict = dp.fetch_daily_delta(tickers_list, EODHD_API_KEY)

        if daily_delta_dict:
            print("Dati incrementali trovati. Eseguo unione nel dizionario...")
            for ticker, delta_df in daily_delta_dict.items():
                print(f"Unione dati per ticker: {ticker}")
                try:
                    if ticker in historical_data_dict:
                        hist_df = historical_data_dict[ticker]
                        
                        # Resetta l'indice di entrambi i DataFrame
                        hist_df_reset = hist_df.reset_index()
                        delta_df_reset = delta_df.reset_index()
                        
                        # Debug: stampa i tipi di dati delle date
                        print(f"  - Tipo di dati 'date' per {ticker} (storico): {hist_df_reset['date'].dtype}")
                        print(f"  - Tipo di dati 'date' per {ticker} (delta): {delta_df_reset['date'].dtype}")
                        
                        # Converti entrambe le colonne date in tz-naive
                        hist_df_reset['date'] = pd.to_datetime(hist_df_reset['date'], utc=True).dt.tz_localize(None)
                        delta_df_reset['date'] = pd.to_datetime(delta_df_reset['date'], utc=True).dt.tz_localize(None)
                        
                        # Concatena e rimuovi duplicati
                        combined_df = pd.concat([hist_df_reset, delta_df_reset], ignore_index=True)
                        combined_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
                        
                        # Reimposta l'indice e ordina
                        combined_df['date'] = pd.to_datetime(combined_df['date'], utc=False)
                        combined_df.set_index('date', inplace=True)
                        combined_df.sort_index(inplace=True)
                        
                        historical_data_dict[ticker] = combined_df
                    else:
                        delta_df.index = pd.to_datetime(delta_df.index, utc=True).tz_localize(None)
                        historical_data_dict[ticker] = delta_df
                except Exception as e:
                    print(f"  - Errore durante l'unione per '{ticker}': {e}")
                    continue
            print("Unione nel dizionario completata.")
        else:
            print("Nessun nuovo dato dall'API.")

        # Controlla se BTC-USD.CC è presente in historical_data_dict
        btc_ticker = 'BTC-USD.CC'
        if btc_ticker not in historical_data_dict:
            print(f"{btc_ticker} non trovato in historical_data_dict. Scarico i dati da EODHD...")
            try:
                # Costruisci l'URL per scaricare i dati storici di Bitcoin
                url = f"https://eodhd.com/api/eod/{btc_ticker}?api_token={EODHD_API_KEY}&fmt=json&period=d"
                response = requests.get(url, timeout=30)
                response.raise_for_status()  # Solleva un'eccezione se la richiesta fallisce
                data = response.json()
                
                if data:
                    # Converte i dati in un DataFrame pandas
                    df = pd.DataFrame(data)
                    
                    # Converti la colonna 'date' in datetime tz-naive
                    df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
                    
                    # Imposta 'date' come indice
                    df.set_index('date', inplace=True)
                    
                    # Mantieni solo le colonne 'close' e 'volume', come per gli altri ticker
                    df = df[['close', 'volume']].dropna()
                    
                    # Aggiungi i dati al dizionario
                    historical_data_dict[btc_ticker] = df
                    print(f"Dati di {btc_ticker} scaricati con successo.")
                else:
                    raise ValueError(f"Nessun dato restituito per {btc_ticker}")
            except Exception as e:
                print(f"Errore nel download dei dati di {btc_ticker}: {e}")
                raise  # Interrompi il workflow se il download fallisce

        # Prepara il DataFrame completo per i panieri
        df_list_for_concat = []
        for ticker, df in historical_data_dict.items():
            df_copy = df.copy()
            df_copy['ticker'] = ticker
            df_list_for_concat.append(df_copy)
        
        full_df_for_baskets = pd.concat(df_list_for_concat).reset_index()
        full_df_for_baskets['date'] = pd.to_datetime(full_df_for_baskets['date'], utc=True).dt.tz_localize(None)
        print(f"Tipo di dati 'date' in full_df_for_baskets: {full_df_for_baskets['date'].dtype}")

        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = dp.create_dynamic_baskets(full_df_for_baskets)
        print(f"Generati {len(dynamic_baskets)} panieri.")

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = dp.calculate_full_asi(historical_data_dict, dynamic_baskets)
        
        if final_asi_df.empty:
            print("ATTENZIONE: Il DataFrame finale dell'ASI è VUOTO.")
        
        print(f"Calcolo ASI completato. Il DataFrame finale ha {len(final_asi_df)} righe.")
        
        # Salva il risultato nella cartella production
        upload_or_update_parquet(gdrive_service, final_asi_df, PRODUCTION_FILE_NAME, production_folder_id)

        print("\n>>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        traceback.print_exc()
        raise
