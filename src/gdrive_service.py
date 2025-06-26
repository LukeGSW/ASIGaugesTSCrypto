# src/gdrive_service.py

import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

def get_gdrive_service(sa_key_string):
    """Crea e restituisce un servizio autenticato per l'API di Google Drive."""
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_key_string))
    service = build('drive', 'v3', credentials=creds)
    return service

# ... qui inseriremo altre funzioni come upload_file, download_file, find_file_id ...
