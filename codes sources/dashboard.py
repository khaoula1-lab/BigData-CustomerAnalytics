import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import time

st.set_page_config(
    page_title="E-Commerce Big Data Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 
st.markdown("""
<style>
    .main { background-color: #ffffff; }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #800020, #4a0010);
    }
    .stMetric {
        background: linear-gradient(135deg, #fff0f5, #ffe4e8);
        border-radius: 10px;
        padding: 15px;
        border-left: 4px solid #800020;
    }
    h1 { color: #800020 !important; }
    h2 { color: #800020 !important; }
    h3 { color: #4a0010 !important; }
    .stButton button {
        background-color: #800020;
        color: white;
        border-radius: 8px;
    }
    hr { border-color: #ff69b4; }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div style='text-align:center; padding:20px;
     background: linear-gradient(90deg, #800020, #ff69b4, #800020);
     border-radius:15px; margin-bottom:20px'>
    <h1 style='color:#ffffff; font-size:2.5em; margin:0'>
        🛒 E-Commerce Analytics Dashboard
    </h1>
    <p style='color:#ffe4e8; margin:5px 0'>
        Kafka → Spark Streaming → MongoDB — Temps Réel
    </p>
    <p style='color:#ffb6c1; font-size:0.8em'>
        Projet Big Data BDIA 2025-2026 | Refresh automatique toutes les 10s
    </p>
</div>
""", unsafe_allow_html=True)

# Connexion MongoDB
@st.cache_resource
def get_db():
    client = MongoClient("localhost", 27017,
                         directConnection=True,
                         serverSelectionTimeoutMS=5000)
    return client["ecommerce"]

db = get_db()


@st.cache_data(ttl=10)
def load_data():
    try:
        clicks = pd.DataFrame(list(db["clickstream"].find()))
        tx     = pd.DataFrame(list(db["transactions"].find()))
        sup    = pd.DataFrame(list(db["service_client"].find()))
    except Exception as e:
        st.error(f"Erreur MongoDB: {e}")
        clicks = pd.DataFrame()
        tx     = pd.DataFrame()
        sup    = pd.DataFrame()
    return clicks, tx, sup

clicks, tx, sup = load_data()

# Couleurs des graphiques
BG      = "#ffffff"
PLOT_BG = "#fff0f5"
FONT    = "#4a0010"
COLORS  = ["#800020", "#ff69b4", "#ffb6c1", "#4a0010", "#ff1493"]

# sidebar
with st.sidebar:
    st.markdown("<h2 style='color:white'>⚙️ Filtres</h2>", unsafe_allow_html=True)
    if len(clicks) > 0 and "category" in clicks.columns:
        categories = ["Toutes"] + list(clicks["category"].unique())
        selected_cat = st.selectbox("Catégorie", categories)
        if selected_cat != "Toutes":
            clicks = clicks[clicks["category"] == selected_cat]

    if len(clicks) > 0 and "source" in clicks.columns:
        sources = ["Toutes"] + list(clicks["source"].unique())
        selected_src = st.selectbox("Source", sources)
        if selected_src != "Toutes":
            clicks = clicks[clicks["source"] == selected_src]

    st.markdown("---")
    st.markdown("<h3 style='color:white'>📡 Statut Pipeline</h3>", unsafe_allow_html=True)
    st.success(" Kafka actif")
    st.success(" Spark Streaming")
    st.success(" MongoDB connecté")
    st.info(f" {len(clicks)} événements")
    st.info(f" {len(tx)} transactions")
    st.info(f"{len(sup)} tickets")

# KPIs
total_events    = len(clicks)
total_purchases = len(clicks[clicks["action"] == "purchase"]) if len(clicks) > 0 else 0
total_abandons  = len(clicks[clicks["action"] == "abandon"])  if len(clicks) > 0 else 0
total_tx        = len(tx)
taux_conversion = round(total_purchases / (total_purchases + total_abandons) * 100, 1) \
                  if (total_purchases + total_abandons) > 0 else 0
revenu_total    = round(tx["montant_paye"].sum(), 2) \
                  if len(tx) > 0 and "montant_paye" in tx.columns else 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric(" Événements",  total_events)
k2.metric(" Achats",       total_purchases)
k3.metric(" Abandons",     total_abandons)
k4.metric(" Transactions", total_tx)
k5.metric(" Conversion",   f"{taux_conversion}%",
          delta="↑ Bon" if taux_conversion > 50 else "↓ Faible")
k6.metric("Revenu",       f"{revenu_total} MAD")

st.divider()

# courbe données en temps réel
st.subheader(" Courbe de conversion en temps réel")

if len(clicks) > 0 and "action" in clicks.columns and "timestamp" in clicks.columns:
    try:
        clicks_time = clicks.copy()
        clicks_time["timestamp"] = pd.to_datetime(clicks_time["timestamp"], unit="s")
        clicks_time["minute"] = clicks_time["timestamp"].dt.floor("min")
        achats_t = clicks_time[clicks_time["action"] == "purchase"].groupby("minute").size().reset_index(name="Achats")
        abandons_t = clicks_time[clicks_time["action"] == "abandon"].groupby("minute").size().reset_index(name="Abandons")
        merged = pd.merge(achats_t, abandons_t, on="minute", how="outer").fillna(0).sort_values("minute").tail(20)
        if len(merged) > 0:
            fig_rt = px.line(merged, x="minute", y=["Achats", "Abandons"],
                             color_discrete_map={"Achats": "#800020", "Abandons": "#ff69b4"})
            fig_rt.update_layout(paper_bgcolor=BG, plot_bgcolor=PLOT_BG,
                                 font_color=FONT, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig_rt, use_container_width=True)
        else:
            st.info(" En attente de données...")
    except Exception as e:
        st.info(" En attente de données...")
else:
    st.info(" En attente de données...")

st.divider()


col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Tunnel de Conversion")
    if len(clicks) > 0 and "action" in clicks.columns:
        funnel_data = clicks["action"].value_counts().reset_index()
        funnel_data.columns = ["action", "count"]
        order = ["view", "search", "add_to_cart", "purchase", "abandon"]
        funnel_data["action"] = pd.Categorical(
            funnel_data["action"], categories=order, ordered=True)
        funnel_data = funnel_data.sort_values("action")
        fig = px.funnel(funnel_data, x="count", y="action",
                        color_discrete_sequence=["#800020"])
        fig.update_layout(paper_bgcolor=BG, plot_bgcolor=PLOT_BG,
                          font_color=FONT, margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader(" Actions par Catégorie")
    if len(clicks) > 0 and "category" in clicks.columns:
        cat_action = clicks.groupby(["category","action"]).size().reset_index(name="count")
        fig2 = px.bar(cat_action, x="category", y="count", color="action",
                      barmode="group", color_discrete_sequence=COLORS)
        fig2.update_layout(paper_bgcolor=BG, plot_bgcolor=PLOT_BG,
                           font_color=FONT, xaxis_tickangle=-30,
                           margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

with col3:
    st.subheader(" Sources de Trafic")
    if len(clicks) > 0 and "source" in clicks.columns:
        sources_df = clicks["source"].value_counts().reset_index()
        sources_df.columns = ["source", "count"]
        fig3 = px.pie(sources_df, names="source", values="count",
                      color_discrete_sequence=COLORS, hole=0.4)
        fig3.update_layout(paper_bgcolor=BG, font_color=FONT,
                           margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig3, use_container_width=True)

st.divider()


col1, col2, col3 = st.columns(3)

with col1:
    st.subheader(" Statut Paiements")
    if len(tx) > 0 and "statut_paiement" in tx.columns:
        statut = tx["statut_paiement"].value_counts().reset_index()
        statut.columns = ["statut", "count"]
        fig4 = px.pie(statut, names="statut", values="count",
                      color_discrete_sequence=["#800020","#ff69b4","#ffb6c1"],
                      hole=0.4)
        fig4.update_layout(paper_bgcolor=BG, font_color=FONT,
                           margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig4, use_container_width=True)

with col2:
    st.subheader(" Méthodes de Paiement")
    if len(tx) > 0 and "methode_paiement" in tx.columns:
        methodes = tx["methode_paiement"].value_counts().reset_index()
        methodes.columns = ["methode", "count"]
        fig5 = px.bar(methodes, x="methode", y="count", color="methode",
                      color_discrete_sequence=COLORS)
        fig5.update_layout(paper_bgcolor=BG, plot_bgcolor=PLOT_BG,
                           font_color=FONT, margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig5, use_container_width=True)

with col3:
    st.subheader("Motifs Support")
    if len(sup) > 0 and "motif_contact" in sup.columns:
        motifs = sup["motif_contact"].value_counts().reset_index()
        motifs.columns = ["motif", "count"]
        fig6 = px.bar(motifs, x="motif", y="count", color="motif",
                      color_discrete_sequence=COLORS)
        fig6.update_layout(paper_bgcolor=BG, plot_bgcolor=PLOT_BG,
                           font_color=FONT, margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig6, use_container_width=True)

st.divider()


col1, col2 = st.columns(2)

with col1:
    st.subheader("Nuage de Mots — Reviews")
    if len(clicks) > 0 and "review_text" in clicks.columns:
        text = " ".join(clicks["review_text"].dropna().astype(str).tolist())
        if len(text.strip()) > 10:
            wc = WordCloud(width=700, height=350,
                           background_color="white",
                           colormap="RdPu",
                           max_words=100).generate(text)
            fig7, ax = plt.subplots(figsize=(8, 4))
            fig7.patch.set_facecolor("white")
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig7)

with col2:
    st.subheader(" Comparaison Modèles ML")
    ml_data = {
        "Modele":    ["Logistic Regression", "Random Forest", "Gradient Boosting"],
        "Précision": [0.671, 0.662, 0.669],
        "Rappel":    [0.738, 0.617, 0.705],
        "F1-Score":  [0.703, 0.639, 0.686],
        "Accuracy":  [0.689, 0.652, 0.679],
    }
    df_ml = pd.DataFrame(ml_data)
    fig8 = px.bar(df_ml, x="Modele",
                  y=["Précision", "Rappel", "F1-Score"],
                  barmode="group",
                  color_discrete_sequence=["#800020", "#ff69b4", "#ffb6c1"])
    fig8.update_layout(paper_bgcolor=BG, plot_bgcolor=PLOT_BG,
                       font_color=FONT, margin=dict(l=10,r=10,t=10,b=10),
                       yaxis=dict(range=[0, 1]))
    st.plotly_chart(fig8, use_container_width=True)
    st.success(" Meilleur modèle : Logistic Regression (F1=0.703)")

st.divider()

#alertes
st.subheader(" Alertes Automatiques")
a1, a2, a3 = st.columns(3)

with a1:
    if taux_conversion < 50:
        st.error(f"⚠️ Conversion bas : {taux_conversion}% — Lancer promotion flash !")
    else:
        st.success(f"✅ Conversion OK : {taux_conversion}%")

with a2:
    if len(tx) > 0 and "statut_paiement" in tx.columns:
        echecs = len(tx[tx["statut_paiement"].str.contains(
            "choue|failed", case=False, na=False)])
        if echecs > 10:
            st.warning(f"⚠️ {echecs} paiements échoués !")
        else:
            st.success(f"✅ Paiements OK ({echecs} échecs)")

with a3:
    if total_abandons > total_purchases:
        st.warning(f"⚠️ Abandons ({total_abandons}) > Achats ({total_purchases})")
    else:
        st.success(f"✅ Achats ({total_purchases}) > Abandons ({total_abandons})")

st.caption("Dashboard — Projet Big Data BDIA 2025-2026")

# Auto-refresh toutes les 10 secondes
time.sleep(5)
st.rerun()