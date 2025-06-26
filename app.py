# app.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# Importiamo le nostre funzioni dai moduli che abbiamo creato
from src.asi_indicator_calculator import calculate_asi_indicators
from src.rule_engine import get_boost_ts1, get_boost_ts2

# --- CONFIGURAZIONE PAGINA ---
st.set_page_config(
    page_title="Kriterion Quant - Allocatore di Capitale",
    page_icon="🤖",
    layout="wide"
)

# --- FUNZIONI HELPER PER LA VISUALIZZAZIONE ---

@st.cache_data
def load_placeholder_data() -> pd.DataFrame:
    """
    Genera un DataFrame fittizio per l'ASI per testare l'UI.
    In futuro, questa funzione verrà sostituita da quella che carica i dati dal cloud.
    """
    dates = pd.to_datetime(pd.date_range(start="2023-01-01", end="2025-05-30", freq="D"))
    # Generiamo un segnale sinusoidale con un po' di rumore per renderlo realistico
    noise = np.random.randn(len(dates)) * 3
    signal = 50 + 45 * np.sin(np.linspace(0, 20, len(dates))) + noise
    signal = np.clip(signal, 0, 100) # Manteniamo i valori tra 0 e 100
    
    asi_df = pd.DataFrame({'index_value': signal}, index=dates)
    return asi_df

def create_gauge_chart(boost_level: str, amount: str, title: str) -> go.Figure:
    """Crea un grafico a tachimetro (gauge) per il livello di boost."""
    
    # Estraiamo il valore numerico dall'importo
    numeric_value = int(amount.replace(',', '').replace(' USD', ''))
    
    color_map = {"Low": "#d9534f", "Standard": "#f0ad4e", "High": "#5cb85c"}
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=numeric_value,
        title={'text': f"<b>{boost_level} BOOST</b>", 'font': {'size': 20}},
        number={'prefix': "$", 'font': {'size': 30}},
        gauge={
            'axis': {'range': [0, 20000], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': color_map.get(boost_level, "darkgray")},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, 5000], 'color': 'rgba(217, 83, 79, 0.2)'},
                {'range': [5000, 10000], 'color': 'rgba(240, 173, 78, 0.2)'},
                {'range': [10000, 15000], 'color': 'rgba(92, 184, 92, 0.2)'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': numeric_value
            }
        }))
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'font': {'size': 24}},
        height=350
    )
    return fig

# --- TITOLO PRINCIPALE ---
st.title("🤖 Kriterion Quant - Cruscotto Allocazione Capitale")
st.markdown("---")

# --- LOGICA DI ORCHESTRAZIONE ---

# 1. Carica i dati (per ora, quelli fittizi)
asi_df = load_placeholder_data()

# 2. Calcola indicatori e fasi
indicators_df = calculate_asi_indicators(asi_df)

# 3. Estrai l'ultima riga per la decisione odierna
latest_data = indicators_df.iloc[-1]

# 4. Applica il motore di regole
level_ts1, amount_ts1, rule_id_ts1 = get_boost_ts1(latest_data)
level_ts2, amount_ts2, rule_id_ts2 = get_boost_ts2(latest_data)


# --- VISUALIZZAZIONE RISULTATI ---

# Sezione di riepilogo dello stato attuale
st.subheader(f"Stato Indicatori al {latest_data.name.strftime('%Y-%m-%d')}")
c1, c2, c3 = st.columns(3)
c1.metric("Fase ASI (SMA30)", latest_data['asi_regime'])
c2.metric("Fase RSI(10)", latest_data['rsi_phase'])
c3.metric("Fase Slope(30)", latest_data['slope_phase'])
st.markdown("---")

# Colonne per i due Trading System
col1, col2 = st.columns(2)

with col1:
    gauge1 = create_gauge_chart(level_ts1, amount_ts1, "Trading System 1: BreakOut Prezzi")
    st.plotly_chart(gauge1, use_container_width=True)
    st.info(f"**Regola Attiva:** {rule_id_ts1}")
    
    with st.expander("Mostra Regole di Allocazione per TS1"):
        st.markdown("""
        **Condizioni HIGH BOOST:**
        1. `ASI SMA='Neutro (20-60)'` AND `RSI(10)SMA30='Forte (>60)'` AND `Slope(30)SMA30='ForteSal(>0.5)'`.
        2. `ASI SMA='Alto (60-100)'` AND `RSI(10)SMA30='Neutro (40-60)'` AND `Slope(30)SMA30='ForteDisc(<-0.5)'`.
        3. `ASI SMA='Neutro (20-60)'` AND `RSI(10)SMA30='Debole (<40)'` AND `Slope(30)SMA30='Lat/Mod(-0.5/0.5)'`.
        4. `ASI SMA='Neutro (20-60)'` AND `RSI(10)SMA30='Debole (<40)'` AND `Slope(30)SMA30='ForteDisc(<-0.5)'`.
        
        **Condizioni LOW BOOST:**
        1. `ASI SMA='Basso (0-20)'` AND `RSI(10)SMA30='Debole (<40)'` AND `Slope(30)SMA30='Lat/Mod(-0.5/0.5)'`.
        2. `ASI SMA='Alto (60-100)'` AND `RSI(10)SMA30='Forte (>60)'` AND `Slope(30)SMA30='ForteSal(>0.5)'`.
        3. `ASI SMA='Alto (60-100)'` AND `RSI(10)SMA30='Neutro (40-60)'` AND `Slope(30)SMA30='Lat/Mod(-0.5/0.5)'`.
        4. `ASI SMA='Basso (0-20)'` AND `RSI(10)SMA30='Neutro (40-60)'` AND `Slope(30)SMA30='Lat/Mod(-0.5/0.5)'`.
        
        **Condizione STANDARD BOOST:** Tutti gli altri casi.
        """)

with col2:
    gauge2 = create_gauge_chart(level_ts2, amount_ts2, "Trading System 2: Break Out Volatilità")
    st.plotly_chart(gauge2, use_container_width=True)
    st.info(f"**Regola Attiva:** {rule_id_ts2}")

    with st.expander("Mostra Regole di Allocazione per TS2"):
        st.markdown("""
        **Condizioni HIGH BOOST:**
        1. `ASI SMA='Neutro (20-60)'` AND `RSI(10)SMA30='Forte (>60)'` AND `Slope(30)SMA30='ForteSal(>0.5)'`.
        2. `ASI SMA='Basso (0-20)'` AND `RSI(10)SMA30='Debole (<40)'` AND `Slope(30)SMA30='ForteDisc(<-0.5)'`.
        3. `ASI SMA='Basso (0-20)'` AND `RSI(10)SMA30='Neutro (40-60)'` AND `Slope(30)SMA30='ForteDisc(<-0.5)'`.
        4. `ASI SMA='Neutro (20-60)'` AND `RSI(10)SMA30='Neutro (40-60)'` AND `Slope(30)SMA30='ForteDisc(<-0.5)'`.
        
        **Condizioni LOW BOOST:**
        1. `ASI SMA='Alto (60-100)'` AND `RSI(10)SMA30='Neutro (40-60)'` AND `Slope(30)SMA30='Lat/Mod(-0.5/0.5)'`.
        2. `ASI SMA='Basso (0-20)'` AND `RSI(10)SMA30='Debole (<40)'` AND `Slope(30)SMA30='Lat/Mod(-0.5/0.5)'`.

        **Condizione STANDARD BOOST:** Tutti gli altri casi.
        """)
        
st.markdown("---")
# --- GRAFICO STORICO ---
st.subheader("Grafico Storico Altcoin Season Index (ASI)")
fig_hist = go.Figure()
fig_hist.add_trace(go.Scatter(x=indicators_df.index, y=indicators_df['index_value'], mode='lines', name='ASI'))
fig_hist.add_trace(go.Scatter(x=indicators_df.index, y=indicators_df['SMA_30'], mode='lines', name='SMA 30 Giorni', line={'dash': 'dash'}))
fig_hist.update_layout(title_text="Andamento Storico dell'ASI e della sua Media Mobile", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
st.plotly_chart(fig_hist, use_container_width=True)
