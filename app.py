import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import traceback
import logging

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Importiamo le nostre funzioni dai moduli
from src.data_loader import load_production_asi
from src.asi_indicator_calculator import calculate_asi_indicators
from src.rule_engine import get_boost_ts1, get_boost_ts2

# --- CONFIGURAZIONE PAGINA ---
st.set_page_config(page_title="Kriterion Quant - Allocatore di Capitale", page_icon="🤖", layout="wide")

def create_gauge_chart(boost_level: str, amount: str, title: str) -> go.Figure:
    numeric_value = int(amount.replace(',', '').replace(' USD', ''))
    color_map = {"Low": "#d9534f", "Standard": "#f0ad4e", "High": "#5cb85c"}
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=numeric_value,
        title={'text': f"<b>{boost_level} BOOST</b>", 'font': {'size': 20}},
        number={'prefix': "$", 'font': {'size': 30}},
        gauge={
            'axis': {'range': [0, 20000], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': color_map.get(boost_level, "darkgray")}, 'bgcolor': "white",
            'borderwidth': 2, 'bordercolor': "gray",
            'steps': [
                {'range': [0, 5000], 'color': 'rgba(217, 83, 79, 0.2)'},
                {'range': [5000, 10000], 'color': 'rgba(240, 173, 78, 0.2)'},
                {'range': [10000, 15000], 'color': 'rgba(92, 184, 92, 0.2)'}],
            'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': numeric_value}}))
    fig.update_layout(title={'text': title, 'x': 0.5, 'font': {'size': 24}}, height=350)
    return fig

# --- TITOLO PRINCIPALE ---
st.title("🤖 Kriterion Quant - Cruscotto Allocazione Capitale")
st.markdown("---")

# --- LOGICA DI ORCHESTRAZIONE ---
data_container = st.empty()
with data_container.container():
    st.info("Caricamento dati di produzione in corso da Google Drive...")
    asi_df = load_production_asi()
    logger.info(f"Dati ASI caricati: {asi_df.shape} righe e colonne")

    # --- INIZIO DELLA PATCH DI SICUREZZA ---
    if asi_df is None or asi_df.empty:
        st.error("Caricamento dati fallito o dati non disponibili. Controllare lo stato della pipeline dati.")
        st.stop()
    
    try:
        if not isinstance(asi_df.index, pd.DatetimeIndex):
            if 'date' in asi_df.columns:
                asi_df['date'] = pd.to_datetime(asi_df['date'])
                asi_df = asi_df.set_index('date')
            else: 
                asi_df.index = pd.to_datetime(asi_df.index)
        
        asi_df.sort_index(inplace=True)
        last_update_str = asi_df.index.max().strftime('%Y-%m-%d')
        st.success(f"Dati caricati con successo. Ultimo aggiornamento: {last_update_str}")
    except Exception as e:
        st.error(f"Errore nella formattazione dei dati caricati: {e}")
        st.code(traceback.format_exc())
        st.stop()
    # --- FINE DELLA PATCH DI SICUREZZA ---

# 2. Esegui i calcoli
indicators_df = calculate_asi_indicators(asi_df)
logger.info(f"Indicatori calcolati: {list(indicators_df.columns)}")
latest_data = indicators_df.iloc[-1]
level_ts1, amount_ts1, rule_id_ts1 = get_boost_ts1(latest_data)
level_ts2, amount_ts2, rule_id_ts2 = get_boost_ts2(latest_data)

# --- LOGGING OPZIONALE DEI VALORI DEI TRADING SYSTEM ---
logger.info(f"Valori per TS1: level={level_ts1}, amount={amount_ts1}, rule={rule_id_ts1}")
logger.info(f"Valori per TS2: level={level_ts2}, amount={amount_ts2}, rule={rule_id_ts2}")

# --- VISUALIZZAZIONE GAUGE ---
st.subheader("Allocazione Attuale per Trading System")
col1, col2 = st.columns(2)
with col1:
    gauge1 = create_gauge_chart(level_ts1, amount_ts1, "Trading System 1")
    st.plotly_chart(gauge1, use_container_width=True)
    st.info(f"Regola Attiva: **{rule_id_ts1}**")

with col2:
    gauge2 = create_gauge_chart(level_ts2, amount_ts2, "Trading System 2")
    st.plotly_chart(gauge2, use_container_width=True)
    st.info(f"Regola Attiva: **{rule_id_ts2}**")

# --- GRAFICI STORICI ---
st.markdown("---")
st.subheader("Analisi Storica degli Indicatori")

tab1, tab2, tab3 = st.tabs(["Storico ASI", "Analisi RSI", "Analisi Slope"])

with tab1:
    st.markdown("Andamento Storico dell'ASI e della sua Media Mobile a 30 giorni.")
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Scatter(x=indicators_df.index, y=indicators_df['index_value'], mode='lines', name='ASI'))
    fig_hist.add_trace(go.Scatter(x=indicators_df.index, y=indicators_df['SMA_30'], mode='lines', name='SMA 30 Giorni', line={'dash': 'dash'}))
    st.plotly_chart(fig_hist, use_container_width=True)

with tab2:
    st.markdown("RSI a 10 periodi calcolato sulla serie storica dell'ASI.")
    fig_rsi = go.Figure()
    fig_rsi.add_trace(go.Scatter(x=indicators_df.index, y=indicators_df['RSI_10'], mode='lines', name='RSI(10) su ASI'))
    fig_rsi.add_hrect(y0=60, y1=100, line_width=0, fillcolor="red", opacity=0.1, layer="below")
    fig_rsi.add_hrect(y0=40, y1=60, line_width=0, fillcolor="yellow", opacity=0.1, layer="below")
    fig_rsi.add_hrect(y0=0, y1=40, line_width=0, fillcolor="green", opacity=0.1, layer="below")
    st.plotly_chart(fig_rsi, use_container_width=True)

with tab3:
    st.markdown("Pendenza (Slope) a 30 periodi calcolata sulla serie storica dell'ASI.")
    fig_slope = go.Figure()
    fig_slope.add_trace(go.Scatter(x=indicators_df.index, y=indicators_df['Slope_30'], mode='lines', name='Slope(30) su ASI'))
    fig_slope.add_hline(y=0, line_dash="dash", line_color="grey")
    st.plotly_chart(fig_slope, use_container_width=True)
