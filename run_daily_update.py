# run_daily_update.py
# Questo script viene eseguito quotidianamente da una GitHub Action.

import os
import io
import pandas as pd
import boto3
import requests
from botocore.exceptions import ClientError

# Importiamo i moduli con la logica di business.
# Supponiamo che questi moduli esistano come pianificato.
import data_processing 

# --- CONFIGURAZIONE ---
# Leggiamo le configurazioni e i segreti dalle variabili d'ambiente
# che verranno impostate dalla GitHub Action.
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
BUCKET_NAME = os.getenv("CLOUDFLARE_BUCKET_NAME")
ENDPOINT_URL = os.getenv("CLOUDFLARE_ENDPOINT_URL")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Definiamo i percorsi nello storage
RAW_HISTORY_PATH = "raw-history/"
PRODUCTION_PATH = "production/altcoin_season_index.parquet"


# --- FUNZIONI HELPER PER LO STORAGE ---

def get_s3_client():
    """Inizializza e restituisce un client S3."""
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name='auto'
    )

def load_raw_history_from_s3(s3_client, bucket, path):
    """Carica tutti i file dalla cartella raw-history e li unisce in un DataFrame."""
    # ... Logica per listare tutti i file in `path` e scaricarli ...
    # ... Questa funzione sarà complessa, per ora la abbozziamo ...
    print(f"Caricamento dati storici da s3://{bucket}/{path}...")
    # In un'implementazione reale, questa funzione scaricherebbe e unirebbe
    # tutti i file .parquet dei singoli ticker.
    # Per ora, supponiamo che restituisca un DataFrame combinato.
    # df_history = ...
    # return df_history
    pass # La implementeremo nel dettaglio dopo


def upload_df_to_s3(s3_client, df, bucket, key):
    """Carica un DataFrame pandas nello storage S3 come file Parquet."""
    try:
        out_buffer = io.BytesIO()
        df.to_parquet(out_buffer, index=True)
        out_buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer)
        print(f"File caricato con successo in s3://{bucket}/{key}")
    except ClientError as e:
        print(f"Errore durante il caricamento su S3: {e}")
        raise

# --- BLOCCO DI ESECUZIONE PRINCIPALE ---

if __name__ == "__main__":
    print(">>> Inizio processo di aggiornamento quotidiano dell'ASI...")

    # Validazione della configurazione
    if not all([EODHD_API_KEY, BUCKET_NAME, ENDPOINT_URL, ACCESS_KEY, SECRET_KEY]):
        raise ValueError("Errore: una o più variabili d'ambiente necessarie non sono state impostate.")

    s3 = get_s3_client()

    try:
        # 1. Carica la base dati storica completa dallo storage
        # Questa è la parte che evita di scaricare tutto ogni giorno.
        # (La logica dettagliata sarà in data_processing.py)
        raw_history_df = data_processing.load_full_history_from_storage(s3, BUCKET_NAME, RAW_HISTORY_PATH)
        print(f"Caricati {len(raw_history_df)} record storici.")
        
        # 2. Scarica solo il "delta" incrementale dall'API
        # (La logica dettagliata sarà in data_processing.py)
        tickers_list = raw_history_df['ticker'].unique().tolist()
        daily_delta_df = data_processing.fetch_daily_delta(tickers_list, EODHD_API_KEY)
        print(f"Scaricati {len(daily_delta_df)} record di aggiornamento dall'API.")
        
        # 3. Unisci i dati storici con il delta
        updated_history_df = pd.concat([raw_history_df, daily_delta_df]).drop_duplicates(subset=['date', 'ticker'], keep='last').sort_values('date')
        print("Dati storici aggiornati in memoria.")

        # 4. Esegui la logica di calcolo principale
        # (Le funzioni sono definite in data_processing.py)
        print("Inizio generazione panieri dinamici...")
        dynamic_baskets = data_processing.create_dynamic_baskets(updated_history_df)
        print(f"Generati {len(dynamic_baskets)} panieri.")

        print("Inizio calcolo Altcoin Season Index...")
        final_asi_df = data_processing.calculate_full_asi(updated_history_df, dynamic_baskets)
        print("Calcolo ASI completato.")

        # 5. Carica il file di produzione aggiornato nello storage
        upload_df_to_s3(s3, final_asi_df, BUCKET_NAME, PRODUCTION_PATH)

        print(">>> Processo di aggiornamento quotidiano completato con successo.")

    except Exception as e:
        print(f"!!! ERRORE CRITICO DURANTE L'ESECUZIONE: {e}")
        # In un sistema di produzione, qui si potrebbe inviare una notifica di errore.
        raise
