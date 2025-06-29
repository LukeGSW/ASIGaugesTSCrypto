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
    baskets = {}
    dates = pd.date_range(start='2018-01-01', end='2025-06-27', freq='D')  # Allinea alla data finale del notebook
    rebalance_dates = pd.date_range(start='2018-01-01', end='2025-06-27', freq=rebalancing_freq)

    for rebalance_date in rebalance_dates:
        lookback_end = rebalance_date - timedelta(days=1)
        lookback_start = lookback_end - timedelta(days=lookback_days)
        if lookback_start < pd.Timestamp('2018-01-01'):
            lookback_start = pd.Timestamp('2018-01-01')
        
        # Filtra i dati per la finestra temporale
        window_data = df.loc[lookback_start:lookback_end].copy()
        if window_data.empty:
            continue
        
        # Calcola il volume totale per ticker
        volume_by_ticker = window_data.groupby('ticker')['volume'].sum()
        top_tickers = volume_by_ticker.nlargest(top_n).index
        
        # Assegna i ticker al paniere per tutte le date fino alla prossima ribilanciamento
        next_rebalance = rebalance_dates[rebalance_dates.get_loc(rebalance_date) + 1] if rebalance_date != rebalance_dates[-1] else pd.Timestamp('2025-06-27')
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
        if len(btc_data) < performance_window:
            btc_perf = 0
        else:
            btc_perf = btc_data.pct_change().mean()
        
        # Calcola la performance media per ogni altcoin nel paniere
        outperforming = 0
        for alt in basket:
            alt_data = historical_data[alt].loc[date - timedelta(days=performance_window):date]
            if len(alt_data) < performance_window or alt_data.isna().all():
                continue
            alt_perf = alt_data.pct_change().mean()
            if alt_perf > btc_perf:
                outperforming += 1
        
        # Calcola l'ASI
        basket_size = min(len(basket), 50)  # Limita a 50 come nel notebook
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
    # Implementazione ipotetica basata sul log
    delta_dict = {}
    for ticker in tickers_list:
        # Simula il fetch (da sostituire con la vera API call)
        delta_dict[ticker] = pd.DataFrame()  # Placeholder
    return delta_dict
