# src/rule_engine.py

import pandas as pd
from typing import Tuple

def get_boost_ts1(latest_indicators_row: pd.Series) -> Tuple[str, str, str]:
    """
    Determina il livello di boost, l'importo e l'ID della regola per il Trading System 1.

    Args:
        latest_indicators_row: Una riga (pd.Series) contenente le fasi degli indicatori
                               (asi_regime, rsi_phase, slope_phase).

    Returns:
        Un tupla contenente (livello_boost, importo_boost, id_regola).
    """
    asi_regime = latest_indicators_row['asi_regime']
    rsi_phase = latest_indicators_row['rsi_phase']
    slope_phase = latest_indicators_row['slope_phase']

    # --- Condizioni HIGH BOOST (TS1) ---
    is_high_boost = (
        (asi_regime == 'Neutro (20-60)' and rsi_phase == 'Forte (>60)' and slope_phase == 'ForteSal(>0.5)') or
        (asi_regime == 'Alto (60-100)' and rsi_phase == 'Neutro (40-60)' and slope_phase == 'ForteDisc(<-0.5)') or
        (asi_regime == 'Neutro (20-60)' and rsi_phase == 'Debole (<40)' and slope_phase == 'Lat/Mod(-0.5/0.5)') or
        (asi_regime == 'Neutro (20-60)' and rsi_phase == 'Debole (<40)' and slope_phase == 'ForteDisc(<-0.5)')
    )
    if is_high_boost:
        # Nota: L'ID regola specifico non è determinabile da questo blocco, ma il livello sì.
        # Per la dashboard, mostreremo il livello e l'elenco delle possibili regole attive.
        return "High", "15,000 USD", "Regole High Boost TS1"

    # --- Condizioni LOW BOOST (TS1) ---
    is_low_boost = (
        (asi_regime == 'Basso (0-20)' and rsi_phase == 'Debole (<40)' and slope_phase == 'Lat/Mod(-0.5/0.5)') or
        (asi_regime == 'Alto (60-100)' and rsi_phase == 'Forte (>60)' and slope_phase == 'ForteSal(>0.5)') or
        (asi_regime == 'Alto (60-100)' and rsi_phase == 'Neutro (40-60)' and slope_phase == 'Lat/Mod(-0.5/0.5)') or
        (asi_regime == 'Basso (0-20)' and rsi_phase == 'Neutro (40-60)' and slope_phase == 'Lat/Mod(-0.5/0.5)')
    )
    if is_low_boost:
        return "Low", "5,000 USD", "Regole Low Boost TS1"

    # --- Condizione STANDARD BOOST (TS1) ---
    return "Standard", "10,000 USD", "TS1-Standard-Default"


def get_boost_ts2(latest_indicators_row: pd.Series) -> Tuple[str, str, str]:
    """
    Determina il livello di boost, l'importo e l'ID della regola per il Trading System 2.

    Args:
        latest_indicators_row: Una riga (pd.Series) contenente le fasi degli indicatori
                               (asi_regime, rsi_phase, slope_phase).

    Returns:
        Un tupla contenente (livello_boost, importo_boost, id_regola).
    """
    asi_regime = latest_indicators_row['asi_regime']
    rsi_phase = latest_indicators_row['rsi_phase']
    slope_phase = latest_indicators_row['slope_phase']

    # --- Condizioni HIGH BOOST (TS2) ---
    is_high_boost = (
        (asi_regime == 'Neutro (20-60)' and rsi_phase == 'Forte (>60)' and slope_phase == 'ForteSal(>0.5)') or
        (asi_regime == 'Basso (0-20)' and rsi_phase == 'Debole (<40)' and slope_phase == 'ForteDisc(<-0.5)') or
        (asi_regime == 'Basso (0-20)' and rsi_phase == 'Neutro (40-60)' and slope_phase == 'ForteDisc(<-0.5)') or
        (asi_regime == 'Neutro (20-60)' and rsi_phase == 'Neutro (40-60)' and slope_phase == 'ForteDisc(<-0.5)')
    )
    if is_high_boost:
        return "High", "15,000 USD", "Regole High Boost TS2"

    # --- Condizioni LOW BOOST (TS2) ---
    is_low_boost = (
        (asi_regime == 'Alto (60-100)' and rsi_phase == 'Neutro (40-60)' and slope_phase == 'Lat/Mod(-0.5/0.5)') or
        (asi_regime == 'Basso (0-20)' and rsi_phase == 'Debole (<40)' and slope_phase == 'Lat/Mod(-0.5/0.5)')
    )
    if is_low_boost:
        return "Low", "5,000 USD", "Regole Low Boost TS2"

    # --- Condizione STANDARD BOOST (TS2) ---
    return "Standard", "10,000 USD", "TS2-Standard-Default"
