# src/gdrive_service.py

import io
import json
import pandas as pd
import time
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

def get_gdrive_service(sa_key_string: str):
    """
    Crea e restituisce un servizio autenticato per l'API di Google Drive.
    Utilizza una stringa JSON contenente le credenziali del service account.
    """
    try:
        creds_info = json.loads(sa_key_string)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        # cache_discovery=False è consigliato in ambienti serverless/contenitori
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        print("Servizio Google Drive autenticato con successo.")
        return service
    except Exception as e:
        print(f"Errore fatale durante l'autenticazione a Google Drive: {e}")
        raise

def find_id(service, name: str, parent_id: str = None, mime_type: str = None) -> Optional[str]:
    """
    Trova l'ID di un file o cartella per nome.
    Può cercare opzionalmente all'interno di una cartella parent e per tipo di file.
    """
    query = f"name = '{name}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    
    try:
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        return files[0].get('id') if files else None
    except HttpError as e:
        print(f"Errore API durante la ricerca di '{name}': {e}")
        return None

def upload_or_update_parquet(service, df: pd.DataFrame, file_name: str, parent_folder_id: str) -> None:
    """
    Carica o aggiorna un DataFrame come file .parquet su Google Drive.
    Include una logica di retry per errori 5xx (errori del server).
    """
    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    
    # Prepara il contenuto del file in un buffer di memoria
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False) # index=False è meglio per i file di dati grezzi
    buffer.seek(0)
    
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    # Controlla se il file esiste già per decidere se aggiornare o creare
    existing_file_id = find_id(service, name=file_name, parent_id=parent_folder_id)
    
    retries = 4
    for i in range(retries):
        try:
            if existing_file_id:
                request = service.files().update(fileId=existing_file_id, media_body=media)
            else:
                request = service.files().create(body=file_metadata, media_body=media, fields='id')
            
            print(f"  - Tentativo di upload per {file_name}...")
            request.execute()
            print(f"  - SUCCESSO: '{file_name}' caricato.")
            return  # Esce dalla funzione se l'upload ha successo
        
        except HttpError as e:
            # Fa un retry solo per errori temporanei del server (5xx)
            if e.resp.status in [500, 502, 503, 504]:
                wait_time = (2 ** i) + 1  # Backoff esponenziale: 2, 3, 5, 9 secondi
                print(f"  - ERRORE SERVER ({e.resp.status}) per '{file_name}'. Riprovo tra {wait_time} secondi... (Tentativo {i+1}/{retries})")
                time.sleep(wait_time)
            else:
                # Per altri errori (es. 403 Forbidden, 404 Not Found), non fa retry.
                print(f"  - ERRORE NON RECUPERABILE ({e.resp.status}) per '{file_name}': {e}")
                return # Interrompe i tentativi per questo specifico file e continua con il prossimo.
    
    print(f"!!! FALLIMENTO FINALE per '{file_name}' dopo {retries} tentativi.")

def download_parquet(service, file_id: str) -> Optional[pd.DataFrame]:
    """Scarica un singolo file .parquet da Google Drive usando il suo ID."""
    try:
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            # print(f"Download: {int(status.progress() * 100)}%.") # Deselezionato per non affollare il log
        
        file_buffer.seek(0)
        df = pd.read_parquet(file_buffer)
        return df
    except HttpError as e:
        print(f"Errore durante il download del file con ID '{file_id}': {e}")
        return None

def download_all_parquets_in_folder(service, folder_id: str) -> pd.DataFrame:
    """
    Scarica tutti i file .parquet da una cartella di GDrive e li unisce in un DataFrame.
    """
    print(f"Ricerca file .parquet nella cartella con ID: {folder_id}...")
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and name contains '.parquet'"
    try:
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        
        if not files:
            raise FileNotFoundError("Nessun file .parquet trovato nella cartella specificata. Eseguire prima il 'full_refresh'.")

        df_list = []
        total_files = len(files)
        print(f"Trovati {total_files} file. Inizio download...")
        for i, file in enumerate(files):
            print(f"  - Download {i+1}/{total_files}: {file.get('name')}")
            df = download_parquet(service, file.get('id'))
            if df is not None:
                df['ticker'] = file.get('name').replace('.parquet', '')
                df_list.append(df)
        
        if not df_list:
             raise ValueError("Nessun dato valido è stato scaricato dalla cartella.")

        full_df = pd.concat(df_list, ignore_index=True)
        full_df['date'] = pd.to_datetime(full_df['date'])
        return full_df

    except Exception as e:
        print(f"Errore durante il download dell'intera cartella: {e}")
        raise
