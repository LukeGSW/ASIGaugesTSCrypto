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
                
                df['date'] = pd.to_datetime(df['date'])
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
    print(">>> Inizio processo di aggiornamento quotidiano dell'ASI (Google Drive)...")
    print(f"Data e ora corrente: {pd.Timestamp.now(tz='Europe/Paris').strftime('%Y-%m-%d %H:%M:%S %Z')}")

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
        root_folder_id = find_id(gdrive_service, name=ROOT
