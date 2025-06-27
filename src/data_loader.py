import streamlit as st
import pandas as pd
import traceback
import io
from googleapiclient.http import MediaIoBaseDownload

from src.gdrive_service import get_gdrive_service, find_id, download_parquet

@st.cache_data(ttl=3600)
def load_production_asi() -> pd.DataFrame:
    """
    Scarica il file Parquet dell'ASI e, in caso di fallimento, ispeziona il contenuto grezzo.
    """
    try:
        sa_key = st.secrets["GDRIVE_SA_KEY"]
        service = get_gdrive_service(sa_key)

        root_folder_id = find_id(service, name="KriterionQuant_Data", mime_type='application/vnd.google-apps.folder')
        if not root_folder_id:
            st.error("Cartella radice 'KriterionQuant_Data' non trovata.")
            return None
            
        prod_folder_id = find_id(service, name="production", parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        if not prod_folder_id:
            st.error("Cartella 'production' non trovata.")
            return None
        
        asi_file_id = find_id(service, name="altcoin_season_index.parquet", parent_id=prod_folder_id)
        if not asi_file_id:
            st.error("File 'altcoin_season_index.parquet' non trovato.")
            return None
        
        # Tentiamo di scaricare e leggere il file Parquet normalmente
        df = download_parquet(service, asi_file_id)

        # Se la funzione restituisce un DataFrame valido e non vuoto, procediamo
        if df is not None and not df.empty:
            st.info("File Parquet letto con successo. Formattazione in corso...")
            if isinstance(df.index, pd.DatetimeIndex):
                df.reset_index(inplace=True)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
                return df
            else:
                st.error("Il DataFrame caricato non ha la colonna 'date'.")
                return pd.DataFrame()
        
        # --- BLOCCO DI ISPEZIONE ---
        # Se df è None o vuoto, significa che download_parquet è fallito.
        # Ora ispezioniamo il file per capire perché.
        st.warning("Lettura del file Parquet fallita. Ispezione del contenuto grezzo del file...")
        
        request = service.files().get_media(fileId=asi_file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        file_buffer.seek(0)
        raw_content = file_buffer.read()
        
        st.error("Il file su Google Drive non è un file Parquet valido o è vuoto.")
        st.subheader("Contenuto Grezzo del File:")
        # Mostriamo il contenuto grezzo per il debug, decodificato come testo.
        st.code(raw_content.decode('utf-8', errors='ignore'))
        st.stop()

    except Exception as e:
        st.error(f"Errore imprevisto durante il caricamento dei dati: {e}")
        st.subheader("Traceback Completo dell'Errore")
        st.code(traceback.format_exc())
        return None
