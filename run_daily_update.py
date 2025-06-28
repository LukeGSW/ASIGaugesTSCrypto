import os
import pandas as pd
import traceback
from src.gdrive_service import get_gdrive_service, find_id, upload_or_update_parquet
import src.data_processing as dp
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaIoBaseDownload
import io

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
GDRIVE_SA_KEY = os.getenv("GDRIVE_SA_KEY")
ROOT_FOLDER_NAME = "KriterionQuant_Data"
PRODUCTION_FOLDER_NAME = "production"
PRODUCTION_FILE_NAME = "altcoin_season_index.parquet"
HIST_FILES_FOLDER_ID = "1_WEblq4NIxkduVaPraiFUd9AniVKV0vJ"  # Sostituisci con l'ID reale di HistFiles

# --- FUNZIONE PER SCARICARE CSV ---
def download_all_csv_in_folder(service: Resource, folder_id: str) -> dict[str, pd.DataFrame]:
    """
    Scarica tutti i file CSV da una cartella di Google Drive e li restituisce come dizionario di DataFrame.
    
    Args:
        service: Servizio Google Drive autenticato.
        folder_id: ID della cartella su Google Drive.
    
    Returns:
        Dizionario con ticker come chiave e DataFrame come valore.
    """
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
                
                # Scarica il file
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
                df['date'] = pd.to_datetime(df['date'], utc=False)
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

# --- BLOCCO DI ESECUZIONE PRINCIPALE ---
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
                        hist_df_reset['date'] = pd.to_datetime(hist_df_reset['date'], utc=False)
                        delta_df_reset['date'] = pd.to_datetime(delta_df_reset['date'], utc=False)
                        
                        # Concatena e rimuovi duplicati
                        combined_df = pd.concat([hist_df_reset, delta_df_reset], ignore_index=True)
                        combined_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
                        
                        # Reimposta l'indice e ordina
                        combined_df['date'] = pd.to_datetime(combined_df['date'], utc=False)
                        combined_df.set_index('date', inplace=True)
                        combined_df.sort_index(inplace=True)
                        
                        historical_data_dict[ticker] = combined_df
                    else:
                        delta_df.index = pd.to_datetime(delta_df.index, utc=False)
                        historical_data_dict[ticker] = delta_df
                except Exception as e:
                    print(f"  - Errore durante l'unione per '{ticker}': {e}")
                    continue
            print("Unione nel dizionario completata.")
        else:
            print("Nessun nuovo dato dall'API.")

        # Prepara il DataFrame completo per i panieri
        df_list_for_concat = []
        for ticker, df in historical_data_dict.items():
            df_copy = df.copy()
            df_copy['ticker'] = ticker
            df_list_for_concat.append(df_copy)
        
        full_df_for_baskets = pd.concat(df_list_for_concat).reset_index()
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
import pandas as pd
import requests
import numpy as np
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

def fetch_daily_delta(tickers: List[str], api_key: str) -> Optional[Dict[str, pd.DataFrame]]:
    print(f"Scaricamento aggiornamenti per {len(tickers)} tickers...")
    delta_dict = {}
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=5)
    
    for ticker in tickers:
        url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&from={start_date}&to={end_date}&fmt=json&period=d"
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data:
                df = pd.DataFrame(data)
                
                final_data = {}
                if 'date' not in df.columns:
                    print(f"  - Nessuna colonna 'date' per {ticker}. Salto.")
                    continue
                
                final_data['date'] = df['date']
                
                if 'adjusted_close' in df.columns and df['adjusted_close'].notna().any():
                    final_data['close'] = df['adjusted_close']
                elif 'close' in df.columns:
                    final_data['close'] = df['close']
                else:
                    print(f"  - Nessuna colonna 'close' valida per {ticker}. Salto.")
                    continue

                if 'volume' in df.columns:
                    final_data['volume'] = df['volume']
                else:
                    print(f"  - Nessuna colonna 'volume' per {ticker}. Salto.")
                    continue
                
                final_df = pd.DataFrame(final_data)
                final_df.dropna(inplace=True)

                if not final_df.empty:
                    final_df['date'] = pd.to_datetime(final_df['date'], utc=False)
                    final_df.set_index('date', inplace=True)
                    delta_dict[ticker] = final_df[['close', 'volume']]
                else:
                    print(f"  - Dati vuoti dopo pulizia per {ticker}. Salto.")

        except requests.exceptions.RequestException as e:
            print(f"  - Errore API per {ticker}: {e}")
            continue
        time.sleep(0.1)

    return delta_dict if delta_dict else None

def create_dynamic_baskets(df: pd.DataFrame, top_n: int = 50, lookback_days: int = 30, rebalancing_freq: str = '90D', performance_window: int = 90) -> Dict:
    if df.empty:
        print("DataFrame vuoto. Nessun paniere generato.")
        return {}
        
    # Converti le date in tz-naive
    df['date'] = pd.to_datetime(df['date'], utc=False)
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    
    start_date = pd.to_datetime('2018-01-01', utc=False)
    end_date = df['date'].max()
    if end_date.tz is not None:
        end_date = end_date.tz_localize(None)
    
    print(f"Creazione panieri dinamici da {start_date} a {end_date}...")
    rebalancing_dates = pd.date_range(start=start_date, end=end_date, freq=rebalancing_freq)
    
    baskets = {}
    btc_ticker = next((t for t in df['ticker'].unique() if 'BTC-USD.CC' in t), None)

    for reb_date in rebalancing_dates:
        lookback_start = reb_date - pd.Timedelta(days=lookback_days)
        
        mask = (df['date'] > lookback_start) & (df['date'] <= reb_date)
        volume_period_df = df.loc[mask]
        
        if volume_period_df.empty:
            print(f"Nessun dato per il periodo {lookback_start} - {reb_date}. Salto.")
            continue
            
        avg_volume = volume_period_df.groupby('ticker')['volume'].mean()
        
        if btc_ticker and btc_ticker in avg_volume.index:
            altcoin_volumes = avg_volume.drop(btc_ticker, errors='ignore')
        else:
            altcoin_volumes = avg_volume
            
        top_by_volume = altcoin_volumes.nlargest(top_n)
        
        final_basket_coins = top_by_volume.index.tolist()
        
        if final_basket_coins:
            baskets[reb_date] = final_basket_coins
        else:
            print(f"Nessun paniere generato per {reb_date}.")
                
    return baskets

def calculate_full_asi(data_dict: Dict[str, pd.DataFrame], baskets: Dict, performance_window: int = 90) -> pd.DataFrame:
    if not data_dict or not baskets:
        print("Dati o panieri vuoti. Restituisco DataFrame vuoto.")
        return pd.DataFrame()

    btc_ticker = next((t for t in data_dict.keys() if 'BTC-USD.CC' in t), None)
    if not btc_ticker or btc_ticker not in data_dict:
        raise ValueError("Dati di Bitcoin non trovati.")
    
    all_dates = pd.to_datetime(sorted(list(set(date for df in data_dict.values() for date in df.index))), utc=False)
    rebalancing_dates = sorted(baskets.keys())
    
    perf_dict = {ticker: df['close'].pct_change(periods=performance_window) for ticker, df in data_dict.items()}
    
    asi_results = []
    
    first_rebal_date = rebalancing_dates[0]
    start_eval_date = first_rebal_date
    
    for eval_date in all_dates[all_dates >= start_eval_date]:
        active_basket_date = next((rd for rd in reversed(rebalancing_dates) if rd <= eval_date), None)
        if not active_basket_date:
            continue
            
        active_basket = baskets.get(active_basket_date, [])
        current_btc_perf = perf_dict[btc_ticker].get(eval_date)
        
        if pd.isna(current_btc_perf):
            continue

        outperforming_count = 0
        valid_alts_in_basket = 0
        for alt in active_basket:
            perf = perf_dict.get(alt, pd.Series(dtype=float)).get(eval_date)
            if pd.notna(perf):
                valid_alts_in_basket += 1
                if perf > current_btc_perf:
                    outperforming_count += 1
        
        if valid_alts_in_basket > 0:
            index_value = (outperforming_count / valid_alts_in_basket) * 100
            asi_results.append({
                'date': eval_date,
                'index_value': index_value,
                'outperforming_count': outperforming_count,
                'basket_size': valid_alts_in_basket
            })

    if not asi_results:
        print("Nessun risultato ASI generato.")
        return pd.DataFrame(columns=['index_value', 'outperforming_count', 'basket_size'])
        
    return pd.DataFrame(asi_results).set_index('date')
