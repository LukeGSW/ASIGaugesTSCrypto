# src/data_loader.py (Versione finale e robusta)

import streamlit as st
import pandas as pd
import traceback

from src.gdrive_service import get_gdrive_service, find_id, download_parquet

@st.cache_data(ttl=3600)
def load_production_asi() -> pd.DataFrame:
    """
    Scarica il file Parquet dell'ASI da Google Drive e ne garantisce il formato.
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
        
        st.info("Caricamento dati di produzione in corso...")
        df = download_parquet(service, asi_file_id)

        # --- INIZIO DELLA CORREZIONE ---
        # Blocco di sicurezza per garantire il formato corretto del DataFrame
        if df is None or df.empty:
            st.warning("Il file di produzione è vuoto o non è stato caricato.")
            return pd.DataFrame() 

        # Se 'date' è già l'indice (ed è un DatetimeIndex), lo spostiamo in una colonna per standardizzare.
        if isinstance(df.index, pd.DatetimeIndex):
             df.reset_index(inplace=True)

        # Ora ci assicuriamo che la colonna 'date' esista, sia del tipo datetime e la impostiamo come indice.
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True) # Ordiniamo l'indice per sicurezza
        else:
            st.error("Il file Parquet caricato non contiene una colonna 'date'.")
            return pd.DataFrame()
        # --- FINE DELLA CORREZIONE ---
        
        return df

    except Exception as e:
        st.error(f"Errore imprevisto durante il caricamento dei dati da Google Drive: {e}")
        st.subheader("Traceback Completo dell'Errore")
        st.code(traceback.format_exc())
        return None
