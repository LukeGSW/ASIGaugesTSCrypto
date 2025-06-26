# src/gdrive_service.py

import io
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

def get_gdrive_service(sa_key_string: str):
    """Crea e restituisce un servizio autenticato per l'API di Google Drive."""
    try:
        creds_info = json.loads(sa_key_string)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        print("Servizio Google Drive autenticato con successo.")
        return service
    except Exception as e:
        print(f"Errore durante l'autenticazione a Google Drive: {e}")
        raise

def find_id(service, name: str, parent_id: str = None, mime_type: str = None) -> str:
    """Trova l'ID di un file o cartella per nome all'interno di un parent."""
    query = f"name = '{name}'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    
    try:
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        if not files:
            return None # Non trovato
        return files[0].get('id')
    except HttpError as e:
        print(f"Errore durante la ricerca di '{name}': {e}")
        return None

def upload_or_update_parquet(service, df: pd.DataFrame, file_name: str, parent_folder_id: str) -> None:
    """Carica o aggiorna un file .parquet su Google Drive."""
    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    
    # Prepara il contenuto del file in memoria
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=True)
    buffer.seek(0)
    
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    # Controlla se il file esiste già per aggiornarlo, altrimenti crealo
    existing_file_id = find_id(service, name=file_name, parent_id=parent_folder_id)
    
    try:
        if existing_file_id:
            request = service.files().update(fileId=existing_file_id, media_body=media)
            print(f"Aggiornamento file: {file_name}...")
        else:
            request = service.files().create(body=file_metadata, media_body=media, fields='id')
            print(f"Creazione nuovo file: {file_name}...")
        
        request.execute()
        print(f"'{file_name}' caricato con successo.")
    except HttpError as e:
        print(f"Errore durante l'upload di '{file_name}': {e}")
        raise

def download_parquet(service, file_id: str) -> pd.DataFrame:
    """Scarica un file .parquet da Google Drive e lo restituisce come DataFrame."""
    try:
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download: {int(status.progress() * 100)}%.")
        
        file_buffer.seek(0)
        df = pd.read_parquet(file_buffer)
        return df
    except HttpError as e:
        print(f"Errore durante il download del file con ID '{file_id}': {e}")
        raise
