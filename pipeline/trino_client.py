"""
Client Trino pour les opérations standardisées.
À placer dans /pipeline/trino_client.py
"""

import trino
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class TrinoClient:
    """Client Trino avec méthodes spécifiques au pipeline procurement."""
    
    def __init__(self, host: str = 'presto_procurement', port: int = 8080, 
                 user: str = 'de_user', catalog: str = 'hive', schema: str = 'raw'):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self._conn = None
    
    @property
    def connection(self):
        if self._conn is None:
            self._conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema
            )
        return self._conn
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Exécute une requête et retourne les résultats comme dictionnaires."""
        cursor = self.connection.cursor()
        try:
            logger.info(f"Exécution: {query[:100]}...")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info(f"→ {len(results)} lignes retournées")
            return results
        except Exception as e:
            logger.error(f"Erreur: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_from_file(self, filepath: str, replacements: Dict[str, str]) -> List[Dict]:
        """
        Exécute une requête depuis un fichier SQL avec remplacement de variables.
        
        Les variables dans le fichier SQL doivent être au format {{KEY}}.
        Exemple: {{EXEC_DATE}} sera remplacé par la valeur de replacements['EXEC_DATE']
        """
        with open(filepath, 'r') as f:
            query = f.read()
        
        logger.info(f"Fichier SQL: {filepath}")
        for key, value in replacements.items():
            pattern = f'{{{{{key}}}}}'  # Remplace {{KEY}} par {{KEY}}
            query = query.replace(pattern, str(value))
            logger.info(f"  Remplacement: {pattern} → {value}")
        
        return self.execute_query(query)
    
    def aggregate_orders(self, exec_date: str) -> int:
        """Agrège les commandes pour une date."""
        sql_path = "/opt/airflow/sql-queries/aggregate_orders.sql"
        self.execute_from_file(sql_path, {'EXEC_DATE': exec_date})
        logger.info(f"Agrégation terminée pour {exec_date}")
        return 0
    
    def calculate_net_demand(self, exec_date: str) -> List[Dict]:
        """Calcule la demande nette pour une date."""
        sql_path = "/opt/airflow/sql-queries/calculate_net_demand.sql"
        return self.execute_from_file(sql_path, {'EXEC_DATE': exec_date})
    
    def sync_partitions(self, table: str, schema: str = 'raw') -> None:
        """Synchronise les partitions Hive."""
        query = f"CALL hive.system.sync_partition_metadata('{schema}', '{table}', 'ADD')"
        self.execute_query(query)
        logger.info(f"Partitions synchronisées pour {schema}.{table}")
    
    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()