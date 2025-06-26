# src/gdrive_service.py

import io
import json
import pandas as pd
import time
import socket
from typing import Optional, Any

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
            # Gestisce errori del server (5xx) e problemi di rete
            status = e.resp.status if isinstance(e, HttpError) else 'N/A'
            if isinstance(e, HttpError) and status < 500:
                 print(f"  - ERRORE CLIENT NON RECUPERABILE ({status}): {e}")
                 raise # Non fare retry per errori 4xx
            
            wait_time = backoff_factor * (2 ** i)
            print(f"  - ERRORE DI RETE/SERVER (Status: {status}). Riprovo tra {wait_time}s... (Tentativo {i+1}/{retries})")
            time.sleep(wait_time)
            
    # Se il loop finisce, tutti i tentativi sono falliti
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
    
    request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
    response = _execute_with_retry(request) # Usa la funzione di retry
    files = response.get('files', [])
    return files[0].get('id') if files else None


def upload_or_update_parquet(service, df: pd.DataFrame, file_name: str, parent_folder_id: str):
    """Carica o aggiorna un DataFrame come file .parquet su Google Drive."""
    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    existing_file_id = find_id(service, name=file_name, parent_id=parent_folder_id)
    
    if existing_file_id:
        request = service.files().update(fileId=existing_file_id, media_body=media)
        print(f"  - Tentativo di aggiornamento per {file_name}...")
    else:
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        print(f"  - Tentativo di creazione per {file_name}...")
        
    _execute_with_retry(request) # Usa la funzione di retry
    print(f"  - SUCCESSO: '{file_name}' gestito.")


# Le altre funzioni (download_parquet, etc.) rimangono le stesse
# e beneficeranno della logica di retry se le chiamate API al loro interno la usano.
# ... (il resto del file rimane invariato) ...
