import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import json

st.set_page_config(page_title="Procurement Pipeline Dashboard", layout="wide")

st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #040D12 0%, #183D3D 100%); }
    .main .block-container {
        background-color: rgba(93, 177, 166, 0.15);
        border-radius: 10px; padding: 2rem;
        backdrop-filter: blur(10px); border: 1px solid #5C8374;
    }
    .stMetric { background-color: #FFFFFF !important; padding: 1rem; border-radius: 8px; border: 1px solid #5C8374; }
    .stMetric > div, .stMetric label { color: #000000 !important; font-weight: bold; }
    [data-testid="stMetricValue"], [data-testid="stMetricLabel"] { color: #000000 !important; }
    .stDataFrame { background-color: rgba(147,177,166,0.2); border-radius: 8px; border: 1px solid #5C8374; }
    h1, h2, h3 { color: #FFFFFF !important; }
    .stTabs [data-baseweb="tab-list"] { background-color: #183D3D; border-radius: 8px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { color: #FFFFFF !important; background-color: transparent; border-radius: 6px; margin: 2px; font-weight: 500; }
    .stTabs [aria-selected="true"] { background-color: #5C8374 !important; color: #FFFFFF !important; font-weight: bold; }
    .stSidebar { background-color: #FFFFFF; }
    .stSidebar h2, .stSidebar h3, .stSidebar p, .stSidebar div, .stSidebar span, .stSidebar label { color: #000000 !important; }
    .stButton > button { background-color: #183D3D; color: #FFFFFF; border: none; border-radius: 6px; font-weight: 500; }
    .stButton > button:hover { background-color: #5C8374; color: #FFFFFF; }
    p, div, span { color: #FFFFFF !important; }
    .stMetric p, .stMetric div, .stMetric span { color: #000000 !important; }
</style>
""", unsafe_allow_html=True)

DB_CONFIG = {
    'host':     os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'procurement_db'),
    'user':     os.getenv('POSTGRES_USER', 'de_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'de_pass'),
    'port': 5432
}

@st.cache_resource
def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"Erreur connexion DB: {e}")
        return None

@st.cache_data(ttl=None)
def load_products():
    conn = get_db_connection()
    if conn:
        try:
            return pd.read_sql("SELECT * FROM products", conn)
        except Exception as e:
            st.error(f"Erreur produits: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=None)
def load_suppliers():
    conn = get_db_connection()
    if conn:
        try:
            return pd.read_sql("SELECT * FROM suppliers", conn)
        except Exception as e:
            st.error(f"Erreur fournisseurs: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=None)
def load_net_demand():
    conn = get_db_connection()
    if conn:
        try:
            return pd.read_sql("SELECT * FROM net_demand ORDER BY created_at DESC", conn)
        except Exception as e:
            st.error(f"Erreur net_demand: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=None)
def load_aggregated_orders():
    conn = get_db_connection()
    if conn:
        try:
            return pd.read_sql("SELECT * FROM aggregated_orders ORDER BY created_at DESC", conn)
        except Exception as e:
            st.error(f"Erreur aggregated_orders: {e}")
    return pd.DataFrame()

def load_supplier_orders():
    """
    Charge les fichiers JSON des commandes fournisseurs.
    Compatible avec le nouveau format structuré:
      { supplier_id, order_date, data_date, items: [{sku, product_name, quantity, unit_cost, total_cost}], total_estimated_cost }
    Et l'ancien format: [{sku, quantity}]
    """
    orders_data = []
    output_dir = "/app/data/output"

    if not os.path.exists(output_dir):
        return pd.DataFrame()

    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if not file.endswith('.json'):
                continue
            file_path = os.path.join(root, file)
            date_folder = os.path.basename(root)

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # ── Nouveau format structuré (avec clé "items") ────────────────
                if isinstance(data, dict) and "items" in data:
                    supplier_id    = data.get("supplier_id", file.replace('.json', ''))
                    supplier_name  = data.get("supplier_name", supplier_id)
                    order_date     = data.get("order_date", date_folder)
                    total_cost     = data.get("total_estimated_cost", 0)

                    for item in data.get("items", []):
                        orders_data.append({
                            'date':             order_date,
                            'data_date':        data.get("data_date", date_folder),
                            'supplier_id':      supplier_id,
                            'supplier_name':    supplier_name,
                            'sku':              item.get('sku', ''),
                            'product_name':     item.get('product_name', item.get('sku', '')),
                            'quantity':         item.get('quantity', 0),
                            'unit_cost':        item.get('unit_cost', 0),
                            'total_cost':       item.get('total_cost', 0),
                            'order_total_cost': total_cost
                        })

                # ── Ancien format liste [{sku, quantity}] ──────────────────────
                elif isinstance(data, list):
                    supplier_id = file.replace('.json', '')
                    for order in data:
                        orders_data.append({
                            'date':          date_folder,
                            'data_date':     date_folder,
                            'supplier_id':   supplier_id,
                            'supplier_name': supplier_id,
                            'sku':           order.get('sku', ''),
                            'product_name':  order.get('sku', ''),
                            'quantity':      order.get('quantity', 0),
                            'unit_cost':     0,
                            'total_cost':    0,
                            'order_total_cost': 0
                        })

            except Exception as e:
                st.error(f"Erreur lecture {file_path}: {e}")

    return pd.DataFrame(orders_data)

# ─── Interface principale ──────────────────────────────────────────────────────
st.title("Procurement Pipeline Dashboard")
st.markdown("---")

st.sidebar.header("Filtres")

# Auto-refresh si Airflow a terminé
flag_file  = '/app/data/.last_update'
check_file = '/app/data/.last_check'
if os.path.exists(flag_file):
    with open(flag_file, 'r') as f:
        current_update = f.read().strip()
    last_check = None
    if os.path.exists(check_file):
        with open(check_file, 'r') as f:
            last_check = f.read().strip()
    if last_check != current_update:
        with open(check_file, 'w') as f:
            f.write(current_update)
        st.cache_data.clear()
        st.rerun()
    st.sidebar.success(f"Dernière mise à jour: {current_update[:19]}")

# ─── Chargement des données ────────────────────────────────────────────────────
products_df  = load_products()
suppliers_df = load_suppliers()
net_demand_df= load_net_demand()
orders_df    = load_supplier_orders()

# ─── Métriques principales ─────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Produits", len(products_df))
with col2:
    st.metric("Fournisseurs", len(suppliers_df))
with col3:
    total_qty = int(orders_df['quantity'].sum()) if not orders_df.empty else 0
    st.metric("Total Unités Commandées", f"{total_qty:,}")
with col4:
    total_cost = orders_df['order_total_cost'].max() if not orders_df.empty and 'order_total_cost' in orders_df.columns else 0
    total_cost_all = orders_df.drop_duplicates(subset=['date','supplier_id'])['order_total_cost'].sum() if not orders_df.empty else 0
    st.metric("Coût Estimé Total", f"{total_cost_all:,.2f} €")

st.markdown("---")

# ─── Onglets ───────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["Vue d'ensemble", "Produits & Fournisseurs", "Commandes", "Analyses"])

with tab1:
    st.header("Vue d'ensemble du Pipeline")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Produits par Fournisseur")
        if not products_df.empty:
            supplier_counts = products_df.groupby('supplier_id').size().reset_index(name='count')
            fig = px.pie(supplier_counts, values='count', names='supplier_id',
                         title="Répartition des produits",
                         color_discrete_sequence=['#1D9E75','#378ADD','#EF9F27','#D4537E'])
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#FFFFFF')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée produit disponible")

    with col2:
        st.subheader("Commandes par Fournisseur")
        if not orders_df.empty:
            supplier_orders = orders_df.groupby('supplier_id')['quantity'].sum().reset_index()
            fig = px.bar(supplier_orders, x='supplier_id', y='quantity',
                         title="Volume de commandes par fournisseur",
                         color_discrete_sequence=['#1D9E75'])
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#FFFFFF')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune commande générée — lancez le pipeline d'abord")

with tab2:
    st.header("Produits & Fournisseurs")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Liste des Produits")
        if not products_df.empty:
            st.dataframe(products_df, use_container_width=True)
        else:
            st.info("Aucun produit trouvé")
    with col2:
        st.subheader("Fournisseurs")
        if not suppliers_df.empty:
            st.dataframe(suppliers_df, use_container_width=True)
        else:
            st.info("Aucun fournisseur trouvé")

with tab3:
    st.header("Commandes Fournisseurs")
    if not orders_df.empty:
        dates = sorted(orders_df['date'].unique(), reverse=True)
        selected_date = st.selectbox("Sélectionner une date", dates)
        filtered = orders_df[orders_df['date'] == selected_date]

        # Résumé coûts
        if 'total_cost' in filtered.columns and filtered['total_cost'].sum() > 0:
            c1, c2, c3 = st.columns(3)
            c1.metric("Unités commandées", int(filtered['quantity'].sum()))
            c2.metric("Coût total estimé", f"{filtered['total_cost'].sum():,.2f} €")
            c3.metric("Fournisseurs", filtered['supplier_id'].nunique())

        col1, col2 = st.columns(2)
        with col1:
            st.subheader(f"Commandes du {selected_date}")
            display_cols = ['supplier_id', 'sku', 'product_name', 'quantity', 'unit_cost', 'total_cost']
            display_cols = [c for c in display_cols if c in filtered.columns]
            st.dataframe(filtered[display_cols], use_container_width=True)
        with col2:
            st.subheader("Répartition par SKU")
            sku_totals = filtered.groupby('sku')['quantity'].sum().reset_index()
            fig = px.bar(sku_totals, x='sku', y='quantity',
                         title=f"Quantités commandées — {selected_date}",
                         color_discrete_sequence=['#1D9E75'])
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#FFFFFF')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune commande disponible. Exécutez le pipeline d'abord.")

with tab4:
    st.header("Analyses Avancées")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Évolution des Demandes")
        if not net_demand_df.empty:
            demand_evolution = net_demand_df.groupby(['exec_date', 'sku'])['total_demand'].sum().reset_index()
            fig = px.line(demand_evolution, x='exec_date', y='total_demand',
                          color='sku', title="Évolution de la demande par produit",
                          color_discrete_sequence=['#1D9E75','#378ADD','#EF9F27'])
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#FFFFFF')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée de demande disponible")

    with col2:
        st.subheader("Stock de Sécurité vs Demande vs Commande")
        if not net_demand_df.empty:
            safety_analysis = net_demand_df[['sku','safety_stock','total_demand','final_order_qty','estimated_cost']].copy()
            # Dédupliquer par SKU (prendre la ligne la plus récente)
            safety_analysis = safety_analysis.drop_duplicates(subset=['sku'], keep='first')
            fig = go.Figure()
            fig.add_trace(go.Bar(name='Stock Sécurité',  x=safety_analysis['sku'], y=safety_analysis['safety_stock'],  marker_color='#378ADD'))
            fig.add_trace(go.Bar(name='Demande Totale',  x=safety_analysis['sku'], y=safety_analysis['total_demand'],  marker_color='#1D9E75'))
            fig.add_trace(go.Bar(name='Commande Finale', x=safety_analysis['sku'], y=safety_analysis['final_order_qty'], marker_color='#EF9F27'))
            fig.update_layout(title="Analyse Stock vs Demande", barmode='group',
                              plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#FFFFFF')
            st.plotly_chart(fig, use_container_width=True)

            # Tableau coûts estimés
            if 'estimated_cost' in safety_analysis.columns and safety_analysis['estimated_cost'].sum() > 0:
                st.subheader("Coûts estimés par SKU")
                cost_display = safety_analysis[['sku','final_order_qty','estimated_cost']].copy()
                cost_display.columns = ['SKU','Quantité commandée','Coût estimé (€)']
                st.dataframe(cost_display, use_container_width=True)

st.markdown("---")
st.markdown("**Procurement Pipeline Dashboard** — Données mises à jour automatiquement après exécution Airflow")