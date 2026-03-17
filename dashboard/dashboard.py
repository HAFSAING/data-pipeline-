import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime, timedelta
import os
import json

st.set_page_config(page_title="Dashboard", layout="wide")

st.markdown("""
<style>
    :root {
        --bg: #213C51;         /* principal */
        --surface: #EEEEEE;    /* surface/cards */
        --accent: #6594B1;     /* secondaire 1 */
        --accent-2: #DDAED3;   /* secondaire 2 */
        --text: #213C51;       /* texte sur surfaces claires */
        --text-on-dark: #EEEEEE;
    }

    /* App background (principal) */
    html, body {
        background-color: var(--bg) !important;
    }
    .stApp {
        background-color: var(--bg) !important;
        background: var(--bg) !important;
    }

    /* Make outer shell dark */
    [data-testid="stAppViewContainer"] { background-color: var(--bg) !important; }
    [data-testid="stHeader"] { background-color: var(--bg) !important; }
    [data-testid="stToolbar"] { background-color: transparent !important; }
    [data-testid="stDecoration"] { background-color: transparent !important; }
    [data-testid="stStatusWidget"] { background-color: transparent !important; }

    /* Default text on dark background (outside cards) */
    body, [data-testid="stAppViewContainer"] { color: var(--text-on-dark) !important; }

    .main .block-container {
        background-color: rgba(238, 238, 238, 0.92);
        border-radius: 14px; padding: 2rem;
        backdrop-filter: blur(6px);
        border: 1px solid rgba(101, 148, 177, 0.35);
        box-shadow: 0 14px 40px rgba(0,0,0,0.35);
    }
    .stMetric { background-color: var(--surface) !important; padding: 1rem; border-radius: 12px; border: 1px solid rgba(33, 60, 81, 0.18); }
    .stMetric > div, .stMetric label { color: var(--text) !important; font-weight: 750; }
    [data-testid="stMetricValue"], [data-testid="stMetricLabel"] { color: var(--text) !important; }

    .stDataFrame { background-color: rgba(238, 238, 238, 0.85); border-radius: 12px; border: 1px solid rgba(33, 60, 81, 0.18); }

    h1, h2, h3 { color: #000000 !important; }
    h1 {
        text-align: center;
        color: var(--text-on-dark) !important;
        background-color: var(--bg) !important;
        padding: 0.5rem 1rem;
        border-radius: 12px;
        display: block;
        width: fit-content;
        margin: 0 auto 1rem auto;
    }

    .stTabs [data-baseweb="tab-list"] { background-color: rgba(101, 148, 177, 0.18); border-radius: 10px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { color: var(--text) !important; background-color: transparent; border-radius: 8px; margin: 2px; font-weight: 650; }
    .stTabs [aria-selected="true"] { background-color: var(--accent) !important; color: var(--text-on-dark) !important; font-weight: 800; }

    /* Sidebar (black) */
    section[data-testid="stSidebar"],
    [data-testid="stSidebarContent"] {
        background-color: var(--bg) !important;
        border-right: 1px solid rgba(101, 148, 177, 0.55) !important;
    }

    /* Sidebar text */
    section[data-testid="stSidebar"] * {
        color: var(--text-on-dark) !important;
    }

    /* Sidebar widgets */
    section[data-testid="stSidebar"] [data-baseweb="select"] > div,
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] textarea {
        background-color: rgba(238, 238, 238, 0.08) !important;
        color: var(--text-on-dark) !important;
        border-color: rgba(101, 148, 177, 0.65) !important;
    }

    /* Sidebar status/alerts (success/info/warning) */
    section[data-testid="stSidebar"] [data-testid="stAlert"] {
        background-color: rgba(238, 238, 238, 0.08) !important;
        border: 1px solid rgba(101, 148, 177, 0.55) !important;
    }
    section[data-testid="stSidebar"] [data-testid="stAlert"] svg {
        fill: var(--accent-2) !important;
    }

    /* Expander */
    section[data-testid="stSidebar"] [data-testid="stExpander"] {
        background-color: rgba(238, 238, 238, 0.06) !important;
        border: 1px solid rgba(101, 148, 177, 0.45) !important;
        border-radius: 10px !important;
    }
    section[data-testid="stSidebar"] [data-testid="stExpander"] summary {
        color: var(--text-on-dark) !important;
    }

    .stButton > button { background-color: var(--accent); color: var(--text-on-dark); border: none; border-radius: 10px; font-weight: 750; }
    .stButton > button:hover { background-color: var(--accent-2); color: var(--bg); }

    /* Main content text stays dark on light surface */
    [data-testid="stMainBlockContainer"] p,
    [data-testid="stMainBlockContainer"] div,
    [data-testid="stMainBlockContainer"] span,
    [data-testid="stMainBlockContainer"] label {
        color: var(--text) !important;
    }
    .stMetric p, .stMetric div, .stMetric span { color: var(--text) !important; }

    /* Alerts in main page: remove Streamlit default blue/green */
    [data-testid="stAlert"] {
        background-color: rgba(238, 238, 238, 0.92) !important;
        border: 1px solid rgba(33, 60, 81, 0.18) !important;
        border-left: 6px solid var(--accent-2) !important;
        border-radius: 12px !important;
    }
    [data-testid="stAlert"] svg { fill: var(--accent-2) !important; }

    /* Selectbox dropdown highlight (avoid blue) */
    [data-baseweb="menu"] {
        background-color: rgba(238, 238, 238, 0.98) !important;
        border: 1px solid rgba(33, 60, 81, 0.18) !important;
    }
    [role="option"][aria-selected="true"] {
        background-color: rgba(221, 174, 211, 0.35) !important;
        color: var(--text) !important;
    }
</style>
""", unsafe_allow_html=True)

THEME = {
    "bg": "#213C51",
    "surface": "#EEEEEE",
    "accent": "#6594B1",
    "accent_2": "#DDAED3",
    "text": "#213C51",
    "accent_soft": "rgba(101,148,177,0.55)",
    "accent_2_soft": "rgba(221,174,211,0.55)",
    "text_soft": "rgba(33,60,81,0.55)",
}

COLORWAY = [
    THEME["accent"],
    THEME["accent_2"],
    THEME["text"],
    THEME["accent_soft"],
    THEME["accent_2_soft"],
    THEME["text_soft"],
]


def apply_plotly_theme(fig, title: str | None = None):
    fig.update_layout(
        template="plotly_white",
        colorway=COLORWAY,
        font_color="#000000",
        title_font=dict(color="#000000"),
        title=title if title is not None else fig.layout.title,
        plot_bgcolor=THEME["surface"],
        paper_bgcolor=THEME["surface"],
        legend=dict(font=dict(color="#000000")),
        xaxis=dict(gridcolor="rgba(34,34,34,0.12)", zerolinecolor="rgba(34,34,34,0.18)"),
        yaxis=dict(gridcolor="rgba(34,34,34,0.12)", zerolinecolor="rgba(34,34,34,0.18)"),
    )
    fig.update_xaxes(title_font=dict(color="#000000"), tickfont=dict(color="#000000"))
    fig.update_yaxes(title_font=dict(color="#000000"), tickfont=dict(color="#000000"))
    return fig


pio.templates.default = "plotly_white"

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


def _safe_parse_iso(dt_str: str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def get_latest_db_timestamps():
    conn = get_db_connection()
    if not conn:
        return {}
    try:
        q = """
        SELECT
          (SELECT MAX(created_at) FROM net_demand)         AS net_demand_max_created_at,
          (SELECT MAX(exec_date)   FROM net_demand)         AS net_demand_max_exec_date,
          (SELECT MAX(created_at) FROM aggregated_orders)  AS aggregated_max_created_at,
          (SELECT MAX(exec_date)   FROM aggregated_orders)  AS aggregated_max_exec_date
        """
        df = pd.read_sql(q, conn)
        if df.empty:
            return {}
        row = df.iloc[0].to_dict()
        return row
    except Exception as e:
        st.sidebar.warning(f"Impossible de lire les timestamps DB: {e}")
        return {}


def get_latest_json_mtime(output_dir: str):
    if not output_dir or not os.path.exists(output_dir):
        return None
    latest = None
    for root, _dirs, files in os.walk(output_dir):
        for file in files:
            if not file.endswith(".json"):
                continue
            fp = os.path.join(root, file)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(fp))
            except Exception:
                continue
            if latest is None or mtime > latest:
                latest = mtime
    return latest

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
st.markdown("<h1>Dashboard</h1>", unsafe_allow_html=True)
st.markdown("---")

st.sidebar.header("Filtres")

# Auto-refresh si Airflow a terminé
flag_file  = '/app/data/.last_update'
check_file = '/app/data/.last_check'
output_dir = "/app/data/output"

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

# Statut de synchronisation (DB + fichiers)
with st.sidebar.expander("Statut de synchronisation", expanded=False):
    last_update_dt = None
    if os.path.exists(flag_file):
        try:
            with open(flag_file, 'r') as f:
                last_update_dt = _safe_parse_iso(f.read().strip())
        except Exception:
            last_update_dt = None

    db_ts = get_latest_db_timestamps()
    json_latest = get_latest_json_mtime(output_dir)

    st.write(f"- **Flag `.last_update`**: `{last_update_dt}`" if last_update_dt else "- **Flag `.last_update`**: (absent)")
    st.write(f"- **Dernier JSON exporté**: `{json_latest}`" if json_latest else "- **Dernier JSON exporté**: (aucun)")

    nd_max = db_ts.get("net_demand_max_created_at")
    ao_max = db_ts.get("aggregated_max_created_at")
    st.write(f"- **DB net_demand MAX(created_at)**: `{nd_max}`" if nd_max else "- **DB net_demand MAX(created_at)**: (vide)")
    st.write(f"- **DB aggregated_orders MAX(created_at)**: `{ao_max}`" if ao_max else "- **DB aggregated_orders MAX(created_at)**: (vide)")

    # Heuristique: si le flag est plus vieux que DB/JSON, Streamlit ne “voit” pas la dernière exécution
    ref_times = [t for t in [json_latest, nd_max, ao_max] if t is not None]
    newest_data = max(ref_times) if ref_times else None
    if last_update_dt and newest_data and last_update_dt + timedelta(seconds=30) < newest_data:
        st.warning("Le flag `.last_update` semble plus ancien que les données (DB/JSON). Le dashboard peut ne pas se rafraîchir automatiquement.")
    elif newest_data and not last_update_dt:
        st.info("Des données existent (DB/JSON) mais `.last_update` est absent. L’auto-refresh ne fonctionnera pas.")
    elif newest_data and last_update_dt:
        st.success("Flag et données semblent cohérents.")

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
                         color_discrete_sequence=[THEME["accent"], THEME["text"], THEME["accent_soft"], THEME["text_soft"]])
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée produit disponible")

    with col2:
        st.subheader("Commandes par Fournisseur")
        if not orders_df.empty:
            supplier_orders = orders_df.groupby('supplier_id')['quantity'].sum().reset_index()
            fig = px.bar(supplier_orders, x='supplier_id', y='quantity',
                         title="Volume de commandes par fournisseur",
                         color_discrete_sequence=[THEME["accent"]])
            apply_plotly_theme(fig)
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
        st.markdown("<div style='color:#000000;font-weight:700;margin-bottom:0.25rem;'>Sélectionner une date</div>", unsafe_allow_html=True)
        selected_date = st.selectbox("Sélectionner une date", dates, label_visibility="collapsed")
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
                         color_discrete_sequence=[THEME["accent"]])
            apply_plotly_theme(fig)
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
                          color_discrete_sequence=[THEME["accent"], THEME["text"], THEME["accent_soft"], THEME["text_soft"]])
            apply_plotly_theme(fig)
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
            fig.add_trace(go.Bar(name='Stock Sécurité',  x=safety_analysis['sku'], y=safety_analysis['safety_stock'],  marker_color=THEME["text_soft"]))
            fig.add_trace(go.Bar(name='Demande Totale',  x=safety_analysis['sku'], y=safety_analysis['total_demand'],  marker_color=THEME["accent_soft"]))
            fig.add_trace(go.Bar(name='Commande Finale', x=safety_analysis['sku'], y=safety_analysis['final_order_qty'], marker_color=THEME["accent"]))
            fig.update_layout(barmode='group')
            apply_plotly_theme(fig, title="Analyse Stock vs Demande")
            st.plotly_chart(fig, use_container_width=True)

            # Tableau coûts estimés
            if 'estimated_cost' in safety_analysis.columns and safety_analysis['estimated_cost'].sum() > 0:
                st.subheader("Coûts estimés par SKU")
                cost_display = safety_analysis[['sku','final_order_qty','estimated_cost']].copy()
                cost_display.columns = ['SKU','Quantité commandée','Coût estimé (€)']
                st.dataframe(cost_display, use_container_width=True)

st.markdown("---")
st.markdown("<div style='color:#000000; font-weight:700;'>Dashboard — Données mises à jour automatiquement après exécution Airflow</div>", unsafe_allow_html=True)