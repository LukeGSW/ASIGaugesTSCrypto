# src/data_loader.py

import streamlit as st
import pandas as pd
# Importiamo il nostro nuovo modulo di servizio
from src.gdrive_service import get_gdrive_service, find_id, download_parquet

@st.cache_data(ttl=3600) # Cache per 1 ora
def load_production_asi() -> pd.DataFrame:
    """
    Scarica il file Parquet dell'ASI da Google Drive.
    """
    try:
        # Recupera la chiave del service account dai secrets di Streamlit
        sa_key = st.secrets["GDRIVE_SA_KEY"]
        
        # Autenticati al servizio
        service = get_gdrive_service(sa_key)

        # Trova gli ID necessari
        # Assumiamo che la cartella radice sia condivisa con il service account
        # e che il suo nome sia univoco. Per robustezza si potrebbe usare l'ID diretto.
        root_folder_id = find_id(service, name="KriterionQuant_Data", mime_type='application/vnd.google-apps.folder')
        if not root_folder_id:
            st.error("Cartella radice 'KriterionQuant_Data' non trovata su Google Drive.")
            return None
            
        prod_folder_id = find_id(service, name="production", parent_id=root_folder_id, mime_type='application/vnd.google-apps.folder')
        if not prod_folder_id:
            st.error("Cartella 'production' non trovata all'interno di 'KriterionQuant_Data'.")
            return None
        
        asi_file_id = find_id(service, name="altcoin_season_index.parquet", parent_id=prod_folder_id)
        if not asi_file_id:
            st.error("File 'altcoin_season_index.parquet' non trovato nella cartella 'production'.")
            return None
        
        # Scarica e restituisci il DataFrame
        st.info("Caricamento dati di produzione in corso...")
        df = download_parquet(service, asi_file_id)
        st.success("Dati caricati con successo.")
        return df

    except Exception as e:
        st.error(f"Errore imprevisto durante il caricamento dei dati da Google Drive: {e}")
        return None
