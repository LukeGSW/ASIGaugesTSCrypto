# src/asi_indicator_calculator.py

import pandas as pd
import numpy as np
# Potremmo usare una libreria come pandas_ta, ma per mantenere le dipendenze al minimo
# e avere pieno controllo, ecco una funzione RSI standard.
def _calculate_rsi(series, period=10):
    """Calcola l'RSI (Relative Strength Index) per una serie di dati."""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _calculate_slope(series, period=30):
    """Calcola la pendenza della retta di regressione lineare."""
    # Creiamo un array di x da 0 a `period-1` per la regressione
    x = np.arange(period)
    
    # Usiamo rolling().apply() per applicare il calcolo della pendenza su finestre mobili
    slopes = series.rolling(window=period).apply(
        lambda y: np.polyfit(x, y, 1)[0] if len(y.dropna()) == period else np.nan,
        raw=False # Passiamo la serie pandas a lambda
    )
    return slopes

def calculate_asi_indicators(asi_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prende la serie storica dell'ASI e calcola tutti gli indicatori derivati e le fasi.

    Args:
        asi_df: DataFrame con una colonna 'index_value' e un DatetimeIndex.

    Returns:
        DataFrame arricchito con le colonne degli indicatori e delle fasi.
    """
    df = asi_df.copy()
    
    # --- 1. Calcolo Indicatori Tecnici ---
    df['SMA_30'] = df['index_value'].rolling(window=30).mean()
    df['RSI_10'] = _calculate_rsi(df['index_value'], period=10)
    df['Slope_30'] = _calculate_slope(df['index_value'], period=30)
    
    # --- 2. Discretizzazione in Fasi/Regimi ---
    
    # Fase ASI basata su SMA_30
    sma_bins = [-np.inf, 20, 60, np.inf]
    sma_labels = ["Basso (0-20)", "Neutro (20-60)", "Alto (60-100)"]
    df['asi_regime'] = pd.cut(df['SMA_30'], bins=sma_bins, labels=sma_labels, right=True)

    # Fase RSI
    rsi_bins = [-np.inf, 39.99, 60, np.inf] # Usiamo 39.99 per includere 40 in 'Neutro'
    rsi_labels = ["Debole (<40)", "Neutro (40-60)", "Forte (>60)"]
    df['rsi_phase'] = pd.cut(df['RSI_10'], bins=rsi_bins, labels=rsi_labels, right=True)

    # Fase Slope
    slope_bins = [-np.inf, -0.5, 0.5, np.inf]
    slope_labels = ["ForteDisc(<-0.5)", "Lat/Mod(-0.5/0.5)", "ForteSal(>0.5)"]
    df['slope_phase'] = pd.cut(df['Slope_30'], bins=slope_bins, labels=slope_labels, right=False) # 'right=False' per allinearsi a 'Lat/Mod(-0.5/0.5)'

    return df
