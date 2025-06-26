# src/gdrive_service.py

import io
import json
import pandas as pd
import time
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
    # (Questa funzione rimane identica)
    query = f"name = '{name}'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    try:
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        return files[0].get('id') if files else None
    except HttpError as e:
        print(f"Errore durante la ricerca di '{name}': {e}")
        return None

def upload_or_update_parquet(service, df: pd.DataFrame, file_name: str, parent_folder_id: str):
    """Carica o aggiorna un file .parquet su Google Drive con logica di retry."""
    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    existing_file_id = find_id(service, name=file_name, parent_id=parent_folder_id)

    # --- NUOVA LOGICA DI RETRY ---
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
            return # Esci dalla funzione se l'upload ha successo
        
        except HttpError as e:
            # Fai un retry solo per errori 5xx (Server Error)
            if e.resp.status in [500, 502, 503, 504]:
                wait_time = (2 ** i) + 1 # Backoff esponenziale: 2, 3, 5, 9 secondi
                print(f"  - ERRORE SERVER ({e.resp.status}) per '{file_name}'. Riprovo tra {wait_time} secondi... (Tentativo {i+1}/{retries})")
                time.sleep(wait_time)
            else:
                # Per altri errori (es. 4xx), non fare retry, segnala e interrompi per questo file
                print(f"  - ERRORE NON RECUPERABILE ({e.resp.status}) per '{file_name}': {e}")
                return # Interrompi i tentativi per questo file
    
    # Se il loop finisce, tutti i tentativi sono falliti
    print(f"!!! FALLIMENTO FINALE per '{file_name}' dopo {retries} tentativi.")


def download_parquet(service, file_id: str) -> pd.DataFrame:
    # (Questa funzione rimane identica)
    # ...

def download_all_parquets_in_folder(service, folder_id: str) -> pd.DataFrame:
    # (Questa funzione rimane identica)
    # ...
