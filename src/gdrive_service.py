# src/gdrive_service.py

import io
import json
import pandas as pd
import time
import socket
from typing import Optional, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# ... (le altre funzioni rimangono uguali) ...

def get_gdrive_service(sa_key_string: str):
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
    query = f"name = '{name}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    
    try:
        request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
        response = request.execute()
        files = response.get('files', [])
        return files[0].get('id') if files else None
    except Exception as e:
        print(f"Errore durante la ricerca di '{name}': {e}")
        return None

def upload_or_update_parquet(service, df: pd.DataFrame, file_name: str, parent_folder_id: str) -> None:
    # Questa funzione rimane uguale
    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    buffer = io.BytesIO()
    # Resettiamo l'indice se è stato modificato, per salvare la data come colonna
    if isinstance(df.index, pd.DatetimeIndex):
        df_to_save = df.reset_index()
    else:
        df_to_save = df
        
    df_to_save.to_parquet(buffer, index=False)
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
        
        request.execute()
        print(f"  - CONFERMATO: '{file_name}' gestito con successo.")
    except Exception as e:
        print(f"!!! FALLIMENTO upload/update per '{file_name}'. Errore: {e}")
        raise

def download_parquet(service, file_id: str) -> Optional[pd.DataFrame]:
    # Questa funzione rimane uguale
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

def download_all_parquets_in_folder(service, folder_id: str) -> Dict[str, pd.DataFrame]:
    """
    Scarica tutti i file .parquet da una cartella e li restituisce come un DIZIONARIO
    di DataFrame, dove la chiave è il ticker.
    """
    print(f"Ricerca file .parquet nella cartella con ID: {folder_id}...")
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and name contains '.parquet'"
    
    try:
        request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
        response = request.execute()
        files = response.get('files', [])
        
        if not files:
            raise FileNotFoundError("Nessun file .parquet trovato. Eseguire prima il 'full_refresh'.")

        data_dict = {}
        total_files = len(files)
        print(f"Trovati {total_files} file. Inizio download...")
        for i, file in enumerate(files):
            print(f"  - Download {i+1}/{total_files}: {file.get('name')}")
            df = download_parquet(service, file.get('id'))
            if df is not None and not df.empty:
                # PULIZIA FONDAMENTALE ALL'ORIGINE
                df.dropna(subset=['date', 'close', 'volume'], inplace=True)
                df.drop_duplicates(subset=['date'], keep='last', inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
                
                ticker = file.get('name').replace('.parquet', '')
                data_dict[ticker] = df
        
        if not data_dict:
             raise ValueError("Nessun dato valido è stato scaricato.")

        print("Colonna 'date' convertita con successo in formato datetime e impostata come indice.")
        return data_dict
    except Exception as e:
        print(f"Errore durante il download dell'intera cartella: {e}")
        raise
