# src/gdrive_service.py

import io
import json
import pandas as pd
import time
import socket
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

def _execute_with_retry(request, retries=5, backoff_factor=2):
    """Esegue una richiesta API con logica di retry e backoff esponenziale."""
    for i in range(retries):
        try:
            return request.execute()
        except (HttpError, socket.timeout, ConnectionResetError) as e:
            status = e.resp.status if isinstance(e, HttpError) else 'N/A'
            if isinstance(e, HttpError) and status < 500:
                 print(f"  - ERRORE CLIENT NON RECUPERABILE ({status}): {e}")
                 raise 
            
            wait_time = backoff_factor * (2 ** i)
            print(f"  - ERRORE DI RETE/SERVER (Status: {status}). Riprovo tra {wait_time}s... (Tentativo {i+1}/{retries})")
            time.sleep(wait_time)
            
    raise Exception(f"La richiesta API è fallita dopo {retries} tentativi.")

def get_gdrive_service(sa_key_string: str):
    """Crea e restituisce un servizio autenticato per l'API di Google Drive."""
    try:
        creds_info = json.loads(sa_key_string)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        print("Servizio Google Drive autenticato con successo.")
        return service
    except Exception as e:
        print(f"Errore fatale durante l'autenticazione a Google Drive: {e}")
        raise

def find_id(service, name: str, parent_id: str = None, mime_type: str = None) -> Optional[str]:
    """Trova l'ID di un file o cartella per nome."""
    query = f"name = '{name}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    
    try:
        request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
        response = _execute_with_retry(request)
        files = response.get('files', [])
        return files[0].get('id') if files else None
    except Exception as e:
        print(f"Errore durante la ricerca di '{name}': {e}")
        return None

def upload_or_update_parquet(service, df: pd.DataFrame, file_name: str, parent_folder_id: str) -> None:
    """Carica o aggiorna un DataFrame come file .parquet su Google Drive."""
    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    existing_file_id = find_id(service, name=file_name, parent_id=parent_folder_id)
    
    try:
        if existing_file_id:
            request = service.files().update(fileId=existing_file_id, media_body=media)
            print(f"  - Tentativo di aggiornamento per {file_name}...")
        else:
            request = service.files().create(body=file_metadata, media_body=media, fields='id')
            print(f"  - Tentativo di creazione per {file_name}...")
        
        _execute_with_retry(request)
        print(f"  - SUCCESSO: '{file_name}' gestito.")
    except Exception as e:
        # La gestione granulare dell'errore è ora in _execute_with_retry
        # Se arriviamo qui, significa che tutti i tentativi sono falliti.
        print(f"!!! FALLIMENTO FINALE per '{file_name}' dopo i tentativi. Errore: {e}")
        raise


def download_parquet(service, file_id: str) -> Optional[pd.DataFrame]:
    """Scarica un singolo file .parquet da Google Drive usando il suo ID."""
    try:
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
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
        request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
        response = _execute_with_retry(request)
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
