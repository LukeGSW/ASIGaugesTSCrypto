# run_full_refresh.py
# Questo script viene eseguito raramente (1-2 volte/anno) tramite trigger manuale
# su GitHub Actions per ricaricare da zero tutta la base dati storica.

import os
import io
import pandas as pd
import boto3
import requests
from botocore.exceptions import ClientError
import time

# --- CONFIGURAZIONE ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
BUCKET_NAME = os.getenv("CLOUDFLARE_BUCKET_NAME")
ENDPOINT_URL = os.getenv("CLOUDFLARE_ENDPOINT_URL")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
RAW_HISTORY_PATH = "raw-history/"
# Codice per l'exchange di crypto su EODHD, es. 'CC' per CryptoCompare
CRYPTO_EXCHANGE_CODE = "CC" 

# --- FUNZIONI HELPER ---

def get_s3_client():
    """Inizializza e restituisce un client S3."""
    return boto3.client('s3', endpoint_url=ENDPOINT_URL, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name='auto')

def upload_ticker_df_to_s3(s3_client, df, bucket, ticker_path):
    """Carica il DataFrame di un singolo ticker nello storage S3 come file Parquet."""
    try:
        out_buffer = io.BytesIO()
        df.to_parquet(out_buffer, index=False) # L'indice non è necessario nel file grezzo
        out_buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=ticker_path, Body=out_buffer)
    except ClientError as e:
        print(f"  - ERRORE S3 durante il caricamento di {ticker_path}: {e}")
        # Non rilanciamo l'eccezione per non bloccare l'intero processo per un file
        
def get_all_tickers(api_key, exchange_code):
    """Recupera la lista di tutti i ticker per un dato exchange da EODHD."""
    print(f"Recupero lista ticker per exchange '{exchange_code}'...")
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json"
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        tickers = [item['Code'] + '.' + exchange_code for item in data if item.get('Type') == 'Common Stock'] # 'Common Stock' è usato anche per le crypto
        print(f"Trovati {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile recuperare la lista dei ticker. {e}")
        raise

def fetch_full_history_for_ticker(ticker, api_key):
    """Scarica la storia completa per un singolo ticker."""
    url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"  - ERRORE API durante il download di {ticker}: {e}")
        return None

# --- BLOCCO DI ESECUZIONE PRINCIPALE ---

if __name__ == "__main__":
    print(">>> Inizio processo di REFRESH COMPLETO dei dati storici...")
    
    if not all([EODHD_API_KEY, BUCKET_NAME, ENDPOINT_URL, ACCESS_KEY, SECRET_KEY]):
        raise ValueError("Errore: una o più variabili d'ambiente necessarie non sono state impostate.")

    s3 = get_s3_client()
    
    # 1. Ottieni la lista completa dei ticker da processare
    all_tickers = get_all_tickers(EODHD_API_KEY, CRYPTO_EXCHANGE_CODE)
    total_tickers = len(all_tickers)
    
    # 2. Itera su ogni ticker, scarica la sua storia e salvala nello storage
    print("\nInizio download e salvataggio dati storici ticker per ticker...")
    for i, ticker in enumerate(all_tickers):
        print(f"Processo {i+1}/{total_tickers}: {ticker}")
        
        # Scarica la storia completa per il ticker
        history_df = fetch_full_history_for_ticker(ticker, EODHD_API_KEY)
        
        if history_df is not None and not history_df.empty:
            # Salva il file .parquet nello storage
            file_key = f"{RAW_HISTORY_PATH}{ticker}.parquet"
            upload_ticker_df_to_s3(s3, history_df, BUCKET_NAME, file_key)
        else:
            print(f"  - Dati non disponibili o vuoti per {ticker}. Salto.")
        
        # Aggiungiamo una piccola pausa per non sovraccaricare l'API
        time.sleep(0.2) 

    print("\n>>> Processo di REFRESH COMPLETO terminato.")
