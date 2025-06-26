# src/data_loader.py

import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Usiamo il caching di Streamlit per scaricare il file una sola volta per sessione
@st.cache_data(ttl=3600) # Cache per 1 ora
def load_production_asi() -> pd.DataFrame:
    """
    Scarica il file Parquet dell'ASI pre-calcolato dallo storage S3-compatible.
    Legge le credenziali e le configurazioni dai secrets di Streamlit.
    """
    try:
        # Recupera i secrets
        endpoint_url = st.secrets["CLOUDFLARE_ENDPOINT_URL"]
        bucket_name = st.secrets["CLOUDFLARE_BUCKET_NAME"]
        access_key = st.secrets["AWS_ACCESS_KEY_ID"]
        secret_key = st.secrets["AWS_SECRET_ACCESS_KEY"]
        
        # Nome del file di produzione
        file_key = "production/altcoin_season_index.parquet"

        # Connessione al client S3
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto' # Spesso 'auto' per Cloudflare R2
        )

        # Scarica l'oggetto in memoria
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # Leggi il file parquet direttamente in un DataFrame pandas
        asi_df = pd.read_parquet(response['Body'])
        
        return asi_df

    except (NoCredentialsError, ClientError, KeyError) as e:
        st.error(f"Errore di connessione allo storage dei dati: {e}")
        st.error("Assicurarsi che i secrets di Streamlit (CLOUDFLARE_*, AWS_*) siano configurati correttamente.")
        return None
    except Exception as e:
        st.error(f"Errore imprevisto durante il caricamento dei dati: {e}")
        return None
