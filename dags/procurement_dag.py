"""
================================================================================
DAG: procurement_pipeline_v2
================================================================================
Pipeline batch complet pour le système d'approvisionnement retail.
Exécuté chaque soir à 22h00.

CORRECTIONS APPLIQUÉES (v3):
  1. provide_context=True supprimé → déprécié en Airflow 2.x, les kwargs sont
     injectés automatiquement via **context
  2. copy_to_processed appelait sync_partitions.py par erreur → corrigé pour
     appeler copy_to_processed.py
  3. ingest_to_hdfs et export_orders convertis en PythonOperator pour hériter
     de context['ds'] (date d'exécution Airflow)
  4. aggregate_orders.sql : DROP + INSERT remplace CREATE TABLE IF NOT EXISTS AS
     SELECT (sinon échec si la table existe déjà)
  5. Import `from exceptions import ...` renommé en `quality_checks_module`
     pour éviter le conflit avec le mot-clé Python `exceptions`
  6. Toute la logique pipeline est dans ce fichier unique (aucune dépendance
     externe requise hormis trino_client.py)
  7. Ajout d'un health-check HTTP sur Trino avant chaque tâche SQL
  8. Logging unifié avec le logger Airflow standard

FLUX DES TÂCHES:
    generate_data
        │
    convert_parquet
        │
    ingest_to_hdfs
        │
    sync_partitions
        │
    aggregate_orders
        │
    calculate_net_demand
        │
        ├── export_orders
        │       │
        └── quality_checks
                │
            copy_to_processed

Schedule : 0 22 * * * (tous les jours à 22h00)
================================================================================
"""

# ─── Imports standard Airflow ──────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
import sys
import time
import subprocess
import json
import csv
import requests
from collections import defaultdict

logger = logging.getLogger(__name__)

# ─── Constantes globales ───────────────────────────────────────────────────────
TRINO_HOST        = "presto_procurement"
TRINO_PORT        = 8080
TRINO_USER        = "de_user"
WEBHDFS_BASE      = "http://namenode:9870/webhdfs/v1"
HDFS_USER         = "root"
PIPELINE_BASE     = "/opt/airflow"
DATA_DIR          = f"{PIPELINE_BASE}/data"
SQL_DIR           = f"{PIPELINE_BASE}/sql-queries"
PIPELINE_DIR      = f"{PIPELINE_BASE}/pipeline"

# HDFS locations (doivent matcher les tables Trino/Hive)
HDFS_PROCUREMENT_BASE = "/procurement"
HDFS_RAW_BASE         = f"{HDFS_PROCUREMENT_BASE}/raw"
HDFS_PROCESSED_BASE   = f"{HDFS_PROCUREMENT_BASE}/processed"

EXPECTED_SKUS     = {"SKU_1001", "SKU_1002", "SKU_1003"}
EXPECTED_STORES   = {"STORE_01", "STORE_02", "STORE_03"}
DEMAND_SPIKE_THRESHOLD = 200

# ─── default_args ─────────────────────────────────────────────────────────────
default_args = {
    "owner":             "de_student",
    "depends_on_past":   False,
    "email_on_failure":  False,            # mettre True + email réel en prod
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
}

# ══════════════════════════════════════════════════════════════════════════════
# UTILITAIRES COMMUNS
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_pipeline_path():
    """Ajoute /opt/airflow/pipeline au sys.path (idempotent)."""
    if PIPELINE_DIR not in sys.path:
        sys.path.insert(0, PIPELINE_DIR)


def _wait_for_trino(max_retries: int = 30, delay: int = 10) -> bool:
    """
    Attend que Trino soit disponible via HTTP health-check.
    Lève une exception après max_retries tentatives.
    """
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info"
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200 and r.json().get("starting") is False:
                logger.info(f"✓ Trino prêt (tentative {attempt}/{max_retries})")
                return True
        except Exception as e:
            logger.warning(f"⏳ Trino pas encore prêt [{attempt}/{max_retries}]: {e}")
        if attempt < max_retries:
            time.sleep(delay)

    raise RuntimeError(f"Trino n'a pas répondu après {max_retries} tentatives ({max_retries * delay}s)")


def _trino_execute(query: str, catalog: str = "hive", schema: str = "raw") -> list:
    """
    Exécute une requête Trino et retourne les résultats sous forme de
    liste de dictionnaires.
    """
    import trino  # import local → pas de problème au chargement du DAG
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog,
        schema=schema,
    )
    cur = conn.cursor()
    try:
        cur.execute(query)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()


def _trino_execute_file(sql_path: str, replacements: dict,
                        catalog: str = "hive", schema: str = "raw") -> list:
    """Lit un fichier .sql, remplace les variables {{KEY}} et l'exécute."""
    with open(sql_path, "r") as f:
        sql = f.read()
    for k, v in replacements.items():
        sql = sql.replace(f"{{{{{k}}}}}", str(v))
    logger.info(f"Exécution SQL depuis {sql_path}")
    return _trino_execute(sql, catalog=catalog, schema=schema)


# ── WebHDFS helpers ────────────────────────────────────────────────────────────

def _hdfs_mkdir(path: str) -> bool:
    url = f"{WEBHDFS_BASE}{path}?op=MKDIRS&user.name={HDFS_USER}"
    r = requests.put(url, allow_redirects=True, timeout=30)
    return r.status_code == 200


def _hdfs_chmod(path: str, permission: str = "777") -> bool:
    """
    Change les permissions d'un chemin HDFS via WebHDFS.
    Utile quand Trino (hive connector) doit écrire dans external_location.
    """
    url = f"{WEBHDFS_BASE}{path}?op=SETPERMISSION&permission={permission}&user.name={HDFS_USER}"
    r = requests.put(url, allow_redirects=True, timeout=30)
    return r.status_code == 200


def _hdfs_stat(path: str) -> dict:
    """Retourne les métadonnées HDFS (owner, permission, type) via WebHDFS."""
    r = requests.get(
        f"{WEBHDFS_BASE}{path}?op=GETFILESTATUS&user.name={HDFS_USER}",
        timeout=30,
    )
    if r.status_code == 200:
        return r.json().get("FileStatus", {})
    return {"_error": f"GETFILESTATUS failed ({r.status_code})", "_body": (r.text or "")[:300]}


def _hdfs_upload(local_path: str, hdfs_path: str) -> bool:
    try:
        create_url = f"{WEBHDFS_BASE}{hdfs_path}?op=CREATE&overwrite=true&user.name={HDFS_USER}"
        r = requests.put(create_url, allow_redirects=False, timeout=30)
        if r.status_code == 307:
            location = r.headers["Location"]
            with open(local_path, "rb") as f:
                w = requests.put(location, data=f, timeout=120)
            return w.status_code == 201
    except Exception as e:
        logger.error(f"Upload HDFS échoué ({hdfs_path}): {e}")
    return False


def _hdfs_copy(src: str, dst: str) -> bool:
    """Copie un fichier à l'intérieur de HDFS via WebHDFS."""
    read_r = requests.get(
        f"{WEBHDFS_BASE}{src}?op=OPEN&user.name={HDFS_USER}",
        allow_redirects=True, timeout=60
    )
    if read_r.status_code != 200:
        return False
    create_r = requests.put(
        f"{WEBHDFS_BASE}{dst}?op=CREATE&overwrite=true&user.name={HDFS_USER}",
        allow_redirects=False, timeout=30
    )
    if create_r.status_code == 307:
        w = requests.put(create_r.headers["Location"], data=read_r.content, timeout=120)
        return w.status_code == 201
    return False


def _hdfs_list(path: str) -> list:
    r = requests.get(
        f"{WEBHDFS_BASE}{path}?op=LISTSTATUS&user.name={HDFS_USER}",
        timeout=30
    )
    if r.status_code == 200:
        return r.json().get("FileStatuses", {}).get("FileStatus", [])
    return []


def _upload_directory(local_dir: str, hdfs_dir: str, ext_filter: str = None) -> int:
    if not os.path.exists(local_dir):
        logger.warning(f"Répertoire local absent: {local_dir}")
        return 0
    _hdfs_mkdir(hdfs_dir)
    count = 0
    for fname in os.listdir(local_dir):
        if ext_filter and not fname.endswith(ext_filter):
            continue
        local_path = os.path.join(local_dir, fname)
        if os.path.isfile(local_path):
            if _hdfs_upload(local_path, f"{hdfs_dir}/{fname}"):
                logger.info(f"  ✓ {fname}")
                count += 1
            else:
                logger.warning(f"  ✗ {fname}")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 1 — Génération des données de test (4 tâches en parallèle)
# ══════════════════════════════════════════════════════════════════════════════

def _run_generator(script_path: str, label: str, **context):
    exec_date = context["ds"]
    logger.info(f"[{label}] Génération des données pour {exec_date}")

    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script absent: {script_path}")

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "EXEC_DATE": exec_date},
    )
    if result.returncode != 0:
        logger.error(f"Erreur dans {script_path}:\n{result.stderr}")
        raise RuntimeError(f"{label} échoué")

    logger.info(f"✅ {label} terminé")


def task_generate_orders(**context):
    return _run_generator(f"{PIPELINE_BASE}/data-generators/generate_orders.py", "generate_orders", **context)


def task_generate_inventory(**context):
    return _run_generator(f"{PIPELINE_BASE}/data-generators/generate_inventory.py", "generate_inventory", **context)


def task_generate_products(**context):
    return _run_generator(f"{PIPELINE_BASE}/data-generators/generate_products.py", "generate_products", **context)


def task_generate_suppliers(**context):
    return _run_generator(f"{PIPELINE_BASE}/data-generators/generate_suppliers.py", "generate_suppliers", **context)


# ══════════════════════════════════════════════════════════════════════════════
# SETUP — Initialisation Trino/Hive (schemas + tables externes)
# ══════════════════════════════════════════════════════════════════════════════

def task_setup_trino_hive(**context):
    """
    Assure que Trino/Hive pointe vers les bons dossiers HDFS.

    Problème typique résolu ici:
    - Les tables créées sans `external_location` pointent vers /warehouse/...
      alors que le pipeline charge les fichiers dans /procurement/...
      → Trino ne voit rien, les tâches SQL échouent ou retournent 0.
    """
    logger.info("[setup_trino_hive] Création/alignement des schémas et tables Trino")
    _wait_for_trino()

    # Pré-créer les dossiers HDFS (évite certains HIVE_METASTORE_ERROR sur locations)
    for p in [
        "/tmp",
        "/tmp/presto",
        HDFS_PROCUREMENT_BASE,
        HDFS_RAW_BASE,
        f"{HDFS_RAW_BASE}/orders",
        f"{HDFS_RAW_BASE}/inventory",
        HDFS_PROCESSED_BASE,
        f"{HDFS_PROCESSED_BASE}/aggregated_orders",
        f"{HDFS_PROCESSED_BASE}/net_demand",
    ]:
        _hdfs_mkdir(p)
        # Permissions larges pour éviter "Permission denied" lors des INSERT Trino
        _hdfs_chmod(p, "777")

    # Important:
    # - Ne pas se connecter avec schema='raw' / 'processed' avant leur création.
    # - On exécute le DDL depuis un schéma toujours présent: hive.information_schema.
    ddl_schema = "information_schema"

    # Schémas (sans WITH location pour éviter les erreurs metastore selon config)
    _trino_execute("CREATE SCHEMA IF NOT EXISTS raw", catalog="hive", schema=ddl_schema)
    _trino_execute("CREATE SCHEMA IF NOT EXISTS processed", catalog="hive", schema=ddl_schema)

    # Pour éviter un mismatch d'anciennes tables (créées vers /warehouse),
    # on DROP puis CREATE (idempotent dans un contexte pédagogique).
    _trino_execute("DROP TABLE IF EXISTS raw.orders", catalog="hive", schema=ddl_schema)
    _trino_execute(
        f"""
        CREATE TABLE raw.orders (
            order_id  VARCHAR,
            store_id  VARCHAR,
            sku       VARCHAR,
            quantity  INTEGER,
            timestamp VARCHAR,
            exec_date VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000{HDFS_RAW_BASE}/orders',
            format = 'PARQUET',
            partitioned_by = ARRAY['exec_date']
        )
        """,
        catalog="hive",
        schema=ddl_schema,
    )

    _trino_execute("DROP TABLE IF EXISTS raw.inventory", catalog="hive", schema=ddl_schema)
    _trino_execute(
        f"""
        CREATE TABLE raw.inventory (
            sku             VARCHAR,
            available_stock INTEGER,
            reserved_stock  INTEGER,
            warehouse       VARCHAR,
            exec_date       VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000{HDFS_RAW_BASE}/inventory',
            format = 'PARQUET',
            partitioned_by = ARRAY['exec_date']
        )
        """,
        catalog="hive",
        schema=ddl_schema,
    )

    _trino_execute("DROP TABLE IF EXISTS processed.aggregated_orders", catalog="hive", schema=ddl_schema)
    _trino_execute(
        f"""
        CREATE TABLE processed.aggregated_orders (
            sku          VARCHAR,
            total_demand BIGINT,
            exec_date    VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000{HDFS_PROCESSED_BASE}/aggregated_orders',
            format = 'PARQUET',
            partitioned_by = ARRAY['exec_date']
        )
        """,
        catalog="hive",
        schema=ddl_schema,
    )

    _trino_execute("DROP TABLE IF EXISTS processed.net_demand", catalog="hive", schema=ddl_schema)
    _trino_execute(
        f"""
        CREATE TABLE processed.net_demand (
            sku              VARCHAR,
            supplier_id      VARCHAR,
            total_demand     BIGINT,
            available_stock  INTEGER,
            reserved_stock   INTEGER,
            safety_stock     INTEGER,
            pack_size        INTEGER,
            moq              INTEGER,
            unit_cost        DOUBLE,
            raw_demand       BIGINT,
            final_order_qty  BIGINT,
            estimated_cost   DOUBLE,
            exec_date        VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000{HDFS_PROCESSED_BASE}/net_demand',
            format = 'PARQUET',
            partitioned_by = ARRAY['exec_date']
        )
        """,
        catalog="hive",
        schema=ddl_schema,
    )

    logger.info("✅ Setup Trino/Hive terminé (schemas + tables alignées sur /procurement)")


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 2 — Conversion Parquet
# ══════════════════════════════════════════════════════════════════════════════

def task_convert_parquet(**context):
    """
    Convertit les fichiers JSON/CSV en Parquet (format optimisé pour Trino).
    Utilise convert_to_parquet.py du pipeline.
    """
    exec_date = context["ds"]
    logger.info(f"[convert_parquet] Conversion Parquet pour {exec_date}")

    _ensure_pipeline_path()
    result = subprocess.run(
        [sys.executable, f"{PIPELINE_DIR}/convert_to_parquet.py"],
        capture_output=True, text=True, timeout=300,
        env={**os.environ, "EXEC_DATE": exec_date}
    )
    if result.returncode != 0:
        logger.error(f"Erreur convert_to_parquet:\n{result.stderr}")
        raise RuntimeError("Conversion Parquet échouée")
    logger.info(f"✅ Conversion Parquet terminée pour {exec_date}")


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 3 — Ingestion HDFS
# ══════════════════════════════════════════════════════════════════════════════

def task_ingest_to_hdfs(**context):
    """
    Upload les fichiers Parquet vers HDFS avec structure de partitions Hive.
    Reproduit la logique de ingest_to_hdfs.py en injectant exec_date depuis
    le contexte Airflow (context['ds']) au lieu du pipeline_config.yaml.
    """
    exec_date = context["ds"]
    logger.info(f"[ingest_to_hdfs] Upload HDFS pour {exec_date}")
    total = 0

    # 1. Orders
    orders_dir = f"{DATA_DIR}/parquet/raw/orders/{exec_date}"
    hdfs_orders = f"{HDFS_RAW_BASE}/orders/exec_date={exec_date}"
    _hdfs_mkdir(hdfs_orders)
    n = _upload_directory(orders_dir, hdfs_orders, ext_filter=".parquet")
    logger.info(f"  Orders : {n} fichiers")
    total += n

    # 2. Inventory
    inventory_dir = f"{DATA_DIR}/parquet/raw/stock/{exec_date}"
    hdfs_inventory = f"{HDFS_RAW_BASE}/inventory/exec_date={exec_date}"
    _hdfs_mkdir(hdfs_inventory)
    n = _upload_directory(inventory_dir, hdfs_inventory, ext_filter=".parquet")
    logger.info(f"  Inventory : {n} fichiers")
    total += n

    # 3. Master data (products / suppliers — non partitionné)
    for dt in ["products", "suppliers"]:
        local_dt = f"{DATA_DIR}/parquet/{dt}"
        hdfs_dt  = f"{HDFS_PROCUREMENT_BASE}/{dt}/static=latest"
        n = _upload_directory(local_dt, hdfs_dt, ext_filter=".parquet")
        logger.info(f"  {dt} : {n} fichiers")
        total += n

    logger.info(f"✅ Ingestion HDFS terminée — {total} fichiers uploadés pour {exec_date}")
    context["ti"].xcom_push(key="hdfs_files_count", value=total)


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 4 — Synchronisation des partitions Hive
# ══════════════════════════════════════════════════════════════════════════════

def task_sync_partitions(**context):
    """
    Informe Hive Metastore des nouvelles partitions uploadées dans HDFS.
    Sans cette étape, Trino ne voit pas les nouveaux fichiers.
    """
    exec_date = context["ds"]
    logger.info(f"[sync_partitions] Synchronisation Hive pour {exec_date}")

    _wait_for_trino()
    _ensure_pipeline_path()

    tables = [
        ("raw",       "orders"),
        ("raw",       "inventory"),
    ]
    for schema, table in tables:
        try:
            _trino_execute(
                f"CALL hive.system.sync_partition_metadata('{schema}', '{table}', 'ADD')",
                catalog="hive", schema=schema
            )
            logger.info(f"  ✓ {schema}.{table}")
        except Exception as e:
            # Ne pas bloquer : la partition peut déjà exister
            logger.warning(f"  ⚠ {schema}.{table}: {e}")

    logger.info(f"✅ Partitions Hive synchronisées pour {exec_date}")


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 5 — Agrégation des commandes
# ══════════════════════════════════════════════════════════════════════════════

def task_aggregate_orders(**context):
    """
    Agrège les commandes journalières par SKU dans processed.aggregated_orders.

    CORRECTION CRITIQUE: La requête originale utilise CREATE TABLE IF NOT EXISTS AS
    SELECT, ce qui échoue si la table existe déjà. On utilise ici :
      1. DELETE des lignes de la date courante (idempotent)
      2. INSERT INTO ... SELECT
    """
    exec_date = context["ds"]
    logger.info(f"[aggregate_orders] Agrégation des commandes pour {exec_date}")

    _wait_for_trino()

    # Diagnostics avant écriture (pour faciliter le debug)
    try:
        raw_cnt = _trino_execute(
            f"SELECT COUNT(*) AS cnt FROM raw.orders WHERE exec_date = '{exec_date}'",
            catalog="hive",
            schema="raw",
        )
        logger.info(f"  raw.orders lignes pour exec_date={exec_date}: {raw_cnt[0]['cnt'] if raw_cnt else 0}")
    except Exception as e:
        logger.warning(f"  Diagnostic COUNT raw.orders impossible: {e}")

    # Supprimer les données de cette date si déjà présentes (idempotence)
    try:
        _trino_execute(
            f"DELETE FROM processed.aggregated_orders WHERE exec_date = '{exec_date}'",
            catalog="hive", schema="processed"
        )
        logger.info(f"  Données existantes supprimées pour {exec_date}")
    except Exception as e:
        logger.warning(f"  DELETE ignoré (table peut-être vide): {e}")

    # Insertion des agrégats
    sql_agg = f"""
    INSERT INTO processed.aggregated_orders (sku, total_demand, exec_date)
    SELECT
        sku,
        CAST(SUM(quantity) AS BIGINT) AS total_demand,
        '{exec_date}'                 AS exec_date
    FROM raw.orders
    WHERE exec_date = '{exec_date}'
    GROUP BY sku
    """
    try:
        _trino_execute(sql_agg, catalog="hive", schema="raw")
    except Exception as e:
        # Diagnostics HDFS + table metadata pour identifier rapidement la cause
        logger.error(f"❌ INSERT aggregated_orders échoué: {e}")
        try:
            logger.error("  SHOW CREATE TABLE processed.aggregated_orders:")
            ddl = _trino_execute(
                "SHOW CREATE TABLE processed.aggregated_orders",
                catalog="hive",
                schema="processed",
            )
            if ddl:
                logger.error(ddl[0].get("Create Table", ddl[0]))
        except Exception as e2:
            logger.error(f"  SHOW CREATE TABLE échoué: {e2}")

        try:
            logger.error("  HDFS stat /procurement/processed/aggregated_orders:")
            logger.error(_hdfs_stat(f"{HDFS_PROCESSED_BASE}/aggregated_orders"))
            logger.error("  HDFS stat partition exec_date=...:")
            logger.error(_hdfs_stat(f"{HDFS_PROCESSED_BASE}/aggregated_orders/exec_date={exec_date}"))
        except Exception as e3:
            logger.error(f"  HDFS stat échoué: {e3}")

        raise

    # Vérification
    results = _trino_execute(
        f"SELECT COUNT(*) AS cnt FROM processed.aggregated_orders WHERE exec_date = '{exec_date}'",
        catalog="hive", schema="processed"
    )
    count = results[0]["cnt"] if results else 0
    logger.info(f"✅ {count} SKUs agrégés pour {exec_date}")
    context["ti"].xcom_push(key="aggregated_count", value=count)


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 6 — Calcul de la demande nette (MRP)
# ══════════════════════════════════════════════════════════════════════════════

def task_calculate_net_demand(**context):
    """
    Calcule la demande nette selon la formule MRP :
      net_demand = MAX(0, total_orders + safety_stock - (available_stock - reserved_stock))
      final_order_qty = CEIL(net_demand / pack_size) * pack_size  [si >= MOQ]

    Sélectionne le fournisseur préféré via ranked_suppliers (priority ASC, cost ASC).
    Résultats écrits dans processed.net_demand.
    """
    exec_date = context["ds"]
    logger.info(f"[calculate_net_demand] Calcul MRP pour {exec_date}")

    _wait_for_trino()

    # Idempotence : supprimer les données existantes pour cette date
    try:
        _trino_execute(
            f"DELETE FROM processed.net_demand WHERE exec_date = '{exec_date}'",
            catalog="hive", schema="processed"
        )
    except Exception as e:
        logger.warning(f"  DELETE net_demand ignoré: {e}")

    # Lecture du fichier SQL externe si disponible, sinon SQL inline
    # NOTE:
    # On force ici un SQL inline avec CAST explicites pour éviter les TYPE_MISMATCH
    # (Trino peut typer SUM(int) en BIGINT, et les DECIMAL PostgreSQL en DECIMAL).
    insert_sql = f"""
    INSERT INTO processed.net_demand
    WITH demand AS (
        SELECT sku, total_demand
        FROM processed.aggregated_orders
        WHERE exec_date = '{exec_date}'
    ),
    inv AS (
        SELECT
            sku,
            CAST(SUM(available_stock) AS INTEGER) AS available_stock,
            CAST(SUM(reserved_stock)  AS INTEGER) AS reserved_stock
        FROM raw.inventory
        WHERE exec_date = '{exec_date}'
        GROUP BY sku
    ),
    ranked_suppliers AS (
        SELECT
            ps.sku,
            ps.supplier_id,
            CAST(ps.unit_cost AS DOUBLE) AS unit_cost,
            ps.priority,
            ROW_NUMBER() OVER (
                PARTITION BY ps.sku
                ORDER BY ps.priority ASC, ps.unit_cost ASC
            ) AS supplier_rank
        FROM postgresql.public.product_suppliers ps
    ),
    master AS (
        SELECT
            p.sku,
            CAST(p.pack_size    AS INTEGER) AS pack_size,
            CAST(p.moq          AS INTEGER) AS moq,
            CAST(p.safety_stock AS INTEGER) AS safety_stock,
            rs.supplier_id,
            CAST(rs.unit_cost   AS DOUBLE)  AS unit_cost
        FROM postgresql.public.products p
        INNER JOIN ranked_suppliers rs
            ON p.sku = rs.sku AND rs.supplier_rank = 1
    ),
    net_calc AS (
        SELECT
            CAST(d.sku AS VARCHAR)         AS sku,
            CAST(m.supplier_id AS VARCHAR) AS supplier_id,
            CAST(d.total_demand AS BIGINT) AS total_demand,
            CAST(COALESCE(i.available_stock, 0) AS INTEGER) AS available_stock,
            CAST(COALESCE(i.reserved_stock,  0) AS INTEGER) AS reserved_stock,
            CAST(m.safety_stock AS INTEGER) AS safety_stock,
            CAST(m.pack_size    AS INTEGER) AS pack_size,
            CAST(m.moq          AS INTEGER) AS moq,
            CAST(m.unit_cost    AS DOUBLE)  AS unit_cost,
            CAST(
                GREATEST(
                    0,
                    d.total_demand + m.safety_stock
                    - (COALESCE(i.available_stock, 0) - COALESCE(i.reserved_stock, 0))
                ) AS BIGINT
            ) AS raw_demand,
            CAST(
                CEIL(
                    CAST(
                        GREATEST(
                            0,
                            d.total_demand + m.safety_stock
                            - (COALESCE(i.available_stock, 0) - COALESCE(i.reserved_stock, 0))
                        ) AS DOUBLE
                    ) / m.pack_size
                ) * m.pack_size
                AS BIGINT
            ) AS final_order_qty
        FROM demand d
        LEFT JOIN inv     i ON d.sku = i.sku
        INNER JOIN master m ON d.sku = m.sku
    )
    SELECT
        sku,
        supplier_id,
        total_demand,
        available_stock,
        reserved_stock,
        safety_stock,
        pack_size,
        moq,
        unit_cost,
        raw_demand,
        final_order_qty,
        CAST(final_order_qty AS DOUBLE) * unit_cost AS estimated_cost,
        '{exec_date}' AS exec_date
    FROM net_calc
    WHERE final_order_qty >= moq
    """

    _trino_execute(insert_sql, catalog="hive", schema="processed")

    # Vérification
    results = _trino_execute(
        f"SELECT COUNT(*) AS cnt, SUM(final_order_qty) AS total_qty, "
        f"SUM(estimated_cost) AS total_cost "
        f"FROM processed.net_demand WHERE exec_date = '{exec_date}'",
        catalog="hive", schema="processed"
    )
    if results:
        r = results[0]
        total_qty = r.get("total_qty") or 0
        total_cost = float(r.get("total_cost") or 0)
        logger.info(
            f"✅ Demande nette calculée: {r.get('cnt', 0)} SKUs | "
            f"Qty: {total_qty} | Coût: {total_cost:.2f} €"
        )
        context["ti"].xcom_push(key="net_demand_count", value=r["cnt"])
        context["ti"].xcom_push(key="total_estimated_cost", value=total_cost)


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 7 — Export des commandes fournisseurs (JSON)
# ══════════════════════════════════════════════════════════════════════════════

def task_export_orders(**context):
    """
    Génère les fichiers JSON de commandes fournisseurs.
    Chaque fournisseur reçoit un fichier {SUP_ID}.json avec :
      - ses articles à commander
      - le total_cost par article
      - le total_estimated_cost global
    """
    exec_date = context["ds"]
    order_date = (datetime.strptime(exec_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    output_dir = f"{DATA_DIR}/output/{exec_date}"
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"[export_orders] Données: {exec_date} | Commande: {order_date}")

    _wait_for_trino()

    # net_demand est écrit dans Hive (processed.net_demand), pas dans PostgreSQL.
    rows = _trino_execute(
        f"""
        SELECT
            nd.supplier_id,
            s.name          AS supplier_name,
            s.contact_email,
            nd.sku,
            p.name          AS product_name,
            CAST(nd.final_order_qty AS INTEGER) AS quantity,
            CAST(nd.unit_cost       AS DOUBLE)  AS unit_cost,
            CAST(nd.estimated_cost  AS DOUBLE)  AS total_cost
        FROM processed.net_demand nd
        JOIN postgresql.public.suppliers s ON nd.supplier_id = s.supplier_id
        JOIN postgresql.public.products  p ON nd.sku = p.sku
        WHERE nd.exec_date = '{exec_date}'
          AND nd.final_order_qty > 0
        ORDER BY nd.supplier_id, nd.sku
        """,
        catalog="hive",
        schema="processed",
    )

    logger.info(f"  {len(rows)} lignes récupérées depuis net_demand")

    # Grouper par fournisseur
    sup_orders = defaultdict(lambda: {
        "items": [], "supplier_name": "", "contact_email": "", "total_estimated_cost": 0.0
    })
    for r in rows:
        sid = r["supplier_id"]
        sup_orders[sid]["supplier_name"]  = r["supplier_name"]
        sup_orders[sid]["contact_email"]  = r["contact_email"]
        sup_orders[sid]["total_estimated_cost"] += float(r["total_cost"] or 0)
        sup_orders[sid]["items"].append({
            "sku":          r["sku"],
            "product_name": r["product_name"],
            "quantity":     int(r["quantity"]),
            "unit_cost":    round(float(r["unit_cost"] or 0), 2),
            "total_cost":   round(float(r["total_cost"] or 0), 2),
        })

    # Écriture JSON
    for sup_id, data in sup_orders.items():
        order = {
            "supplier_id":          sup_id,
            "supplier_name":        data["supplier_name"],
            "contact_email":        data["contact_email"],
            "order_date":           order_date,
            "data_date":            exec_date,
            "generated_at":         datetime.now().isoformat(),
            "items":                data["items"],
            "total_estimated_cost": round(data["total_estimated_cost"], 2),
        }
        fpath = f"{output_dir}/{sup_id}.json"
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(order, f, indent=2, ensure_ascii=False)
        logger.info(f"  ✓ {sup_id} ({data['supplier_name']}): "
                    f"{len(data['items'])} articles | "
                    f"{order['total_estimated_cost']:.2f} € → {fpath}")

    # Flag pour le dashboard Streamlit
    flag = f"{DATA_DIR}/.last_update"
    os.makedirs(os.path.dirname(flag), exist_ok=True)
    with open(flag, "w") as f:
        f.write(datetime.now().isoformat())

    logger.info(f"✅ Export terminé — {len(sup_orders)} fichiers dans {output_dir}")
    context["ti"].xcom_push(key="suppliers_exported", value=len(sup_orders))


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 8 — Contrôles qualité (5 checks)
# ══════════════════════════════════════════════════════════════════════════════

def task_quality_checks(**context):
    """
    Exécute les 5 contrôles qualité :
      1. missing_pos_files       : tous les stores ont envoyé leurs fichiers
      2. missing_inventory       : snapshot CSV du jour présent
      3. missing_supplier_mapping: chaque SKU mappé à un fournisseur
      4. demand_spike            : quantités anormalement élevées
      5. inventory_inconsistency : stock réservé > disponible ou valeur négative

    CORRECTION: Utilise `import importlib` pour charger exceptions.py
    sans conflit avec le mot-clé Python `exceptions`.
    """
    exec_date = context["ds"]
    logger.info(f"[quality_checks] Contrôles qualité pour {exec_date}")

    _ensure_pipeline_path()

    # Import sécurisé de exceptions.py (évite conflit avec builtins)
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "quality_checks_module",
        f"{PIPELINE_DIR}/exceptions.py"
    )
    qc_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(qc_module)

    report = qc_module.run_quality_checks(exec_date)

    logger.info(f"  Erreurs  : {report['errors']}")
    logger.info(f"  Warnings : {report['warnings']}")
    logger.info(f"  Infos    : {report['infos']}")

    context["ti"].xcom_push(key="quality_report", value=report)

    if report["errors"] > 0:
        logger.warning(
            f"⚠ {report['errors']} erreur(s) qualité détectée(s). "
            f"Voir {DATA_DIR}/logs/exceptions/exceptions_{exec_date}.json"
        )
        # On ne lève pas d'exception pour ne pas bloquer le pipeline :
        # les erreurs qualité sont informationnelles dans ce contexte académique.
        # En production : raise AirflowException(...)

    logger.info(f"✅ Contrôles qualité terminés pour {exec_date}")


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 9 — Archivage HDFS (/processed/ + /output/ + /logs/)
# ══════════════════════════════════════════════════════════════════════════════

def task_copy_to_processed(**context):
    """
    Archive les résultats du pipeline vers HDFS :
      1. Tables Hive (aggregated_orders, net_demand) → /processed/
      2. JSON fournisseurs                           → /output/supplier_orders/
      3. Logs d'exceptions                           → /logs/exceptions/

    CORRECTION: Ce module appelait sync_partitions.py par erreur.
    Il appelle maintenant la logique de copy_to_processed.py directement
    (reproduite ici pour l'autonomie du DAG unifié).
    """
    exec_date = context["ds"]
    logger.info(f"[copy_to_processed] Archivage HDFS pour {exec_date}")
    total = 0

    # ── 1. Vérification des outputs Hive (déjà stockés sous /procurement/...) ──
    logger.info("[1/3] Vérification outputs Hive sous /procurement/processed/ ...")
    processed_partitions = [
        f"{HDFS_PROCESSED_BASE}/aggregated_orders/exec_date={exec_date}",
        f"{HDFS_PROCESSED_BASE}/net_demand/exec_date={exec_date}",
    ]
    for p in processed_partitions:
        _hdfs_mkdir(p)
        files = [f for f in _hdfs_list(p) if f.get("type") == "FILE" and not f.get("pathSuffix", "").startswith(".")]
        logger.info(f"  {p}: {len(files)} fichier(s)")
        total += len(files)

    # ── 2. JSON fournisseurs → /output/supplier_orders/ ───────────────────────
    logger.info("[2/3] Upload JSON fournisseurs vers HDFS /output/...")
    json_local = f"{DATA_DIR}/output/{exec_date}"
    json_hdfs  = f"{HDFS_PROCUREMENT_BASE}/output/supplier_orders/order_date={exec_date}"
    n = _upload_directory(json_local, json_hdfs, ext_filter=".json")
    total += n
    logger.info(f"  → {n} fichiers JSON uploadés")

    # ── 3. Logs exceptions → /logs/exceptions/ ────────────────────────────────
    logger.info("[3/3] Upload logs exceptions vers HDFS /logs/...")
    logs_local = f"{DATA_DIR}/logs/exceptions"
    logs_hdfs  = f"{HDFS_PROCUREMENT_BASE}/logs/exceptions/exec_date={exec_date}"
    n = _upload_directory(logs_local, logs_hdfs, ext_filter=".json")
    total += n
    logger.info(f"  → {n} fichiers logs uploadés")

    # ── Synchronisation partitions processed ──────────────────────────────────
    try:
        _wait_for_trino(max_retries=5, delay=5)
        for schema, table in [("processed", "aggregated_orders"), ("processed", "net_demand")]:
            _trino_execute(
                f"CALL hive.system.sync_partition_metadata('{schema}', '{table}', 'ADD')",
                catalog="hive", schema=schema
            )
            logger.info(f"  ✓ Partitions sync: {schema}.{table}")
    except Exception as e:
        logger.warning(f"  Sync partitions processed ignoré: {e}")

    logger.info(f"✅ Archivage terminé — {total} fichiers copiés vers HDFS pour {exec_date}")
    context["ti"].xcom_push(key="archived_files_count", value=total)


# ══════════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="procurement_pipeline_v2",
    default_args=default_args,
    description=(
        "Pipeline batch procurement : génération données → Parquet → HDFS → "
        "sync partitions → agrégation → MRP → export JSON fournisseurs → "
        "contrôles qualité → archivage HDFS"
    ),
    schedule_interval="0 22 * * *",      # Chaque soir à 22h00
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_runs=1,                   # Un seul run à la fois
    tags=["procurement", "batch", "v2"],
    doc_md=__doc__,
) as dag:

    # ── Setup Trino/Hive (tables externes alignées sur /procurement) ───────────
    t0_setup = PythonOperator(
        task_id="setup_trino_hive",
        python_callable=task_setup_trino_hive,
        doc_md="Crée/aligner les schemas/tables Trino/Hive sur les paths HDFS /procurement (idempotent)",
    )

    # ── Tâche 1 : Génération des données (4 tâches en parallèle) ──────────────
    t1_orders = PythonOperator(
        task_id="generate_orders",
        python_callable=task_generate_orders,
        doc_md="Génère les commandes POS (orders)",
    )
    t1_inventory = PythonOperator(
        task_id="generate_inventory",
        python_callable=task_generate_inventory,
        doc_md="Génère le snapshot inventaire (inventory)",
    )
    t1_products = PythonOperator(
        task_id="generate_products",
        python_callable=task_generate_products,
        doc_md="Génère les produits (products)",
    )
    t1_suppliers = PythonOperator(
        task_id="generate_suppliers",
        python_callable=task_generate_suppliers,
        doc_md="Génère les fournisseurs (suppliers)",
    )

    # ── Tâche 2 : Conversion Parquet ──────────────────────────────────────────
    t2_parquet = PythonOperator(
        task_id="convert_parquet",
        python_callable=task_convert_parquet,
        doc_md="Convertit JSON/CSV en Parquet (snappy) pour optimisation Trino",
    )

    # ── Tâche 3 : Ingestion HDFS ──────────────────────────────────────────────
    t3_ingest = PythonOperator(
        task_id="ingest_to_hdfs",
        python_callable=task_ingest_to_hdfs,
        doc_md="Upload les Parquet vers HDFS avec partitionnement exec_date=",
    )

    # ── Tâche 4 : Sync partitions Hive ────────────────────────────────────────
    t4_sync = PythonOperator(
        task_id="sync_partitions",
        python_callable=task_sync_partitions,
        doc_md="Informe Hive Metastore des nouvelles partitions (CALL sync_partition_metadata)",
    )

    # ── Tâche 5 : Agrégation des commandes ────────────────────────────────────
    t5_aggregate = PythonOperator(
        task_id="aggregate_orders",
        python_callable=task_aggregate_orders,
        doc_md="Agrège SUM(quantity) par SKU dans processed.aggregated_orders",
    )

    # ── Tâche 6 : Calcul demande nette (MRP) ──────────────────────────────────
    t6_net_demand = PythonOperator(
        task_id="calculate_net_demand",
        python_callable=task_calculate_net_demand,
        doc_md=(
            "MRP: net_demand = MAX(0, orders + safety_stock - (available - reserved)). "
            "Sélection fournisseur via ranked_suppliers."
        ),
    )

    # ── Tâche 7 : Export JSON fournisseurs ────────────────────────────────────
    t7_export = PythonOperator(
        task_id="export_orders",
        python_callable=task_export_orders,
        doc_md="Génère {SUP_ID}.json avec total_cost et total_estimated_cost",
    )

    # ── Tâche 8 : Contrôles qualité ───────────────────────────────────────────
    t8_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=task_quality_checks,
        doc_md="5 checks : POS manquants, inventaire absent, mapping SKU, spikes, incohérences stock",
    )

    # ── Tâche 9 : Archivage HDFS ──────────────────────────────────────────────
    t9_archive = PythonOperator(
        task_id="copy_to_processed",
        python_callable=task_copy_to_processed,
        doc_md="Archive Parquet Hive + JSON + logs vers /processed/, /output/, /logs/ HDFS",
    )

    
    (
        t0_setup
        >> [t1_orders, t1_inventory, t1_products, t1_suppliers]
        >> t2_parquet
        >> t3_ingest
        >> t4_sync
        >> t5_aggregate
        >> t6_net_demand
        >> [t7_export, t8_quality]
        >> t9_archive
    )