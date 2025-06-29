import pandas as pd
import logging
from datetime import timedelta

# Configura il logging
logger = logging.getLogger(__name__)

def create_dynamic_baskets(df, top_n=50, lookback_days=30, rebalancing_freq='90D'):
    """
    Crea panieri dinamici di altcoin basati sul volume su una finestra temporale.
    Parametri:
        df: DataFrame con i dati storici (indice temporale, colonne: ticker, close, volume)
        top_n: Numero di altcoin da includere nei panieri
        lookback_days: Finestra temporale per calcolare il volume
        rebalancing_freq: Frequenza di ribilanciamento dei panieri (es. '90D' per 90 giorni)
    """
    logger.info(f"Parametri panieri: top_n={top_n}, lookback_days={lookback_days}, rebalancing_freq={rebalancing_freq}")
    
    # Verifica che l'indice sia un DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("L'indice del DataFrame deve essere un DatetimeIndex")
    
    # Definisci il range delle date basato sui dati disponibili
    start_date = df.index.min()
    end_date = df.index.max()
    dates = pd.date_range(start=max(start_date, pd.Timestamp('2018-05-01')), 
                          end=min(end_date, pd.Timestamp('2025-06-27')), 
                          freq='D')
    rebalance_dates = pd.date_range(start=dates.min(), end=dates.max(), freq=rebalancing_freq)

    baskets = {}
    for rebalance_date in rebalance_dates:
        lookback_end = rebalance_date - timedelta(days=1)
        lookback_start = lookback_end - timedelta(days=lookback_days)
        
        # Assicurati che le date siano nell'intervallo del DataFrame
        if lookback_start < df.index.min():
            lookback_start = df.index.min()
        if lookback_end > df.index.max():
            lookback_end = df.index.max()
        
        # Filtra i dati per la finestra temporale
        window_data = df.loc[lookback_start:lookback_end].copy()
        if window_data.empty:
            logger.warning(f"Nessun dato disponibile per la finestra {lookback_start} a {lookback_end}")
            continue
        
        # Calcola il volume totale per ticker
        volume_by_ticker = window_data.groupby('ticker')['volume'].sum()
        top_tickers = volume_by_ticker.nlargest(top_n).index
        
        # Assegna i ticker al paniere per tutte le date fino alla prossima ribilanciamento
        next_rebalance = rebalance_dates[rebalance_dates.get_loc(rebalance_date) + 1] if rebalance_date != rebalance_dates[-1] else end_date
        dates_in_range = dates[(dates >= rebalance_date) & (dates < next_rebalance)]
        for date in dates_in_range:
            baskets[date.strftime('%Y-%m-%d')] = top_tickers.tolist()
    
    logger.info(f"Panieri generati: {len(baskets)} panieri, esempio per 2025-06-27: {baskets.get('2025-06-27')}")
    return baskets

def calculate_full_asi(historical_data, baskets, performance_window=90):
    """
    Calcola l'ASI basato sulla performance delle altcoin rispetto a Bitcoin.
    Parametri:
        historical_data: DataFrame con i dati storici (indice temporale, colonne: ticker)
        baskets: Dizionario dei panieri dinamici
        performance_window: Finestra temporale per calcolare la performance (in giorni)
    """
    logger.info(f"Finestra performance ASI: {performance_window}")
    asi_df = pd.DataFrame(index=historical_data.index)
    
    for date in historical_data.index:
        basket = baskets.get(date.strftime('%Y-%m-%d'), [])
        if len(basket) == 0:
            continue
        
        # Calcola la performance media di Bitcoin su 90 giorni
        btc_data = historical_data['BTC-USD.CC'].loc[date - timedelta(days=performance_window):date]
        if len(btc_data) < performance_window or btc_data.isna().all():
            logger.warning(f"Dati insufficienti per Bitcoin a {date}")
            continue
        btc_perf = btc_data.pct_change().mean()
        
        # Calcola la performance media per ogni altcoin nel paniere
        outperforming = 0
        basket_size = min(len(basket), 50)  # Limita a 50 come nel notebook
        for alt in basket:
            alt_data = historical_data[alt].loc[date - timedelta(days=performance_window):date]
            if len(alt_data) < performance_window or alt_data.isna().all():
                continue
            alt_perf = alt_data.pct_change().mean()
            if alt_perf > btc_perf:
                outperforming += 1
        
        # Calcola l'ASI
        asi = (outperforming / basket_size) * 100 if basket_size > 0 else 0
        asi_df.loc[date, 'index_value'] = asi
        asi_df.loc[date, 'outperforming_count'] = outperforming
        asi_df.loc[date, 'basket_size'] = basket_size
        
        logger.info(f"ASI per {date}: {asi:.2f}% ({outperforming}/{basket_size} alts sovraperformanti)")
    
    # Verifica il valore finale per confronto con il notebook
    if '2025-06-27' in asi_df.index:
        logger.info(f"ASI finale per 2025-06-27: {asi_df.loc['2025-06-27', 'index_value']:.2f}%")
    
    return asi_df

# Funzione di supporto per il fetch dei dati giornalieri (ipotizzata da run_daily_update.py)
def fetch_daily_delta(tickers_list, api_key):
    """
    Recupera i dati giornalieri incrementali tramite API EODHD.
    Parametri:
        tickers_list: Lista dei ticker da aggiornare
        api_key: Chiave API per EODHD
    """
    import requests
    delta_dict = {}
    for ticker in tickers_list:
        try:
            url = f"https://eodhd.com/api/eod/{ticker}?api_token={api_key}&fmt=json&period=d"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
                df.set_index('date', inplace=True)
                df = df[['close', 'volume']].dropna()
                delta_dict[ticker] = df
            else:
                logger.warning(f"Nessun dato restituito per {ticker}")
        except Exception as e:
            logger.error(f"Errore nel fetch di {ticker}: {e}")
    return delta_dict
