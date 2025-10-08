"""
Create relational assets (Database, Schema, Tables) under an existing connection.

Config section used: relational_assets

relational_assets:
  enabled: true
  # Provide either connection_qualified_name OR connection_name
  connection_qualified_name: "default/snowflake/12345"
  connection_name: "Snowflake-Prod"   # used if qualified name not provided
  database: "EXAMPLE_DB"
  schemas:
    - name: "PUBLIC"
      tables:
        - name: "SAMPLE_TABLE"
          description: "Created by automation"
        - name: "ANOTHER_TABLE"
"""

import os
import logging
import argparse
from typing import Dict, Optional, List

import yaml
from dotenv import load_dotenv

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, Database, Schema, Table
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery


load_dotenv()

LOGS_DIR = os.path.join(os.getcwd(), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'relational_assets_creation.log')),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def _read_yaml_config(path: Optional[str]) -> Dict:
    if not path:
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.error(f"Config file not found: {path}")
        return {}
    except Exception as e:
        logger.error(f"Failed to read config file '{path}': {e}")
        return {}


def _extract_atlan_credentials(config: Dict) -> Dict[str, Optional[str]]:
    base_url = None
    api_key = None

    atlan_cfg = config.get('atlan') if isinstance(config, dict) else None
    if isinstance(atlan_cfg, dict):
        base_url = atlan_cfg.get('base_url') or atlan_cfg.get('BASE_URL')
        api_key = atlan_cfg.get('api_key') or atlan_cfg.get('API_KEY')

    if not base_url:
        base_url = config.get('base_url') or config.get('BASE_URL')
    if not api_key:
        api_key = config.get('api_key') or config.get('API_KEY')

    return {"base_url": base_url, "api_key": api_key}


def _initialize_client(config: Dict) -> Optional[AtlanClient]:
    creds = _extract_atlan_credentials(config)
    api_key = creds.get('api_key') or os.getenv('API_KEY')
    base_url = creds.get('base_url') or os.getenv('BASE_URL')
    if not api_key or not base_url:
        logger.error("API_KEY and BASE_URL must be available via config or environment.")
        return None
    return AtlanClient(api_key=api_key, base_url=base_url)


def _resolve_connection_qn(client: AtlanClient, cfg: Dict) -> Optional[str]:
    qn = (cfg.get('connection_qualified_name') or '').strip()
    if qn:
        return qn
    name = (cfg.get('connection_name') or '').strip()
    if not name:
        return None
    try:
        request = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Connection))
            .where(Connection.NAME.eq(name))
            .page_size(1)
            .to_request()
        )
        response = client.asset.search(request)
        for c in response:
            if isinstance(c, Connection) and c.name == name:
                return c.qualified_name
    except Exception as e:
        logger.error(f"Failed to resolve connection by name '{name}': {e}")
    return None


def create_relational_assets(client: AtlanClient, config: Dict) -> bool:
    rcfg = config.get('relational_assets') or {}
    if not isinstance(rcfg, dict) or not rcfg.get('enabled'):
        logger.info("relational_assets not enabled. Nothing to do.")
        return True

    connection_qn = _resolve_connection_qn(client, rcfg)
    if not connection_qn:
        logger.error("Provide 'connection_qualified_name' or 'connection_name' under relational_assets.")
        return False

    database_name = (rcfg.get('database') or '').strip()
    if not database_name:
        logger.error("relational_assets.database is required when enabled.")
        return False

    logger.info(f"Creating Database '{database_name}' under {connection_qn}...")
    db = Database.creator(name=database_name, connection_qualified_name=connection_qn)
    client.asset.save(db)

    schemas: List[Dict] = rcfg.get('schemas') or []
    if not schemas:
        logger.info("No schemas specified. Done after database creation.")
        return True

    for s in schemas:
        schema_name = (s.get('name') or '').strip()
        if not schema_name:
            continue
        logger.info(f"Creating Schema '{schema_name}'...")
        sch = Schema.creator(name=schema_name, database_qualified_name=f"{connection_qn}/{database_name}")
        client.asset.save(sch)

        for t in s.get('tables') or []:
            t_name = (t.get('name') or '').strip()
            if not t_name:
                continue
            logger.info(f"Creating Table '{t_name}'...")
            table = Table.creator(
                name=t_name,
                schema_qualified_name=f"{connection_qn}/{database_name}/{schema_name}",
                description=(t.get('description') or None),
            )
            client.asset.save(table)

    logger.info("Relational assets creation completed.")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Create relational assets under a connection")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        required=False,
        help="Path to YAML config file containing Atlan credentials and relational_assets settings",
    )
    args = parser.parse_args()

    config = _read_yaml_config(args.config)
    client = _initialize_client(config)
    if not client:
        return 1

    ok = create_relational_assets(client, config)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


