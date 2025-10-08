"""
Create a custom Atlan connection (and optional relational assets) via pyatlan.

Reads settings from YAML config (same file as lineage), under key: custom_connection

Config example:

custom_connection:
  name: "my-custom-conn"
  connector:
    # Either one of known types by name (e.g., SNOWFLAKE) or define a custom type
    # If custom is provided, this script will register it dynamically.
    use: "CUSTOM"             # CUSTOM or KNOWN
    known_type: "API"         # Optional, if using a known connector enum by name
    custom:
      name: "MY_CUSTOM"
      value: "my-custom"
      category: "API"         # One of: API, STORAGE, DATABASE, BI, ORCHESTRATION, ML, OTHER

  admin_roles:
    - "$admin"               # names of roles to grant as admins (resolved to GUIDs)

  create_assets:
    enabled: true
    database: "EXAMPLE_DB"
    schema: "PUBLIC"
    tables:
      - name: "SAMPLE_TABLE"
        description: "Created by automation"

"""

import os
import logging
import argparse
from typing import Dict, Optional, List

import yaml
from dotenv import load_dotenv

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, Database, Schema, Table
from pyatlan.model.enums import AtlanConnectorType, AtlanConnectionCategory


load_dotenv()

LOGS_DIR = os.path.join(os.getcwd(), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'connection_creation.log')),
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


def _resolve_admin_roles(client: AtlanClient, role_names: List[str]) -> List[str]:
    resolved: List[str] = []
    for role_name in role_names:
        try:
            guid = client.role_cache.get_id_for_name(role_name)
            if guid:
                resolved.append(guid)
            else:
                logger.warning(f"Could not resolve role '{role_name}' to a GUID.")
        except Exception as e:
            logger.warning(f"Failed resolving role '{role_name}': {e}")
    return resolved


def _get_connector_type(cfg: Dict) -> AtlanConnectorType:
    connector_cfg = cfg.get('connector', {}) if isinstance(cfg, dict) else {}
    use_mode = (connector_cfg.get('use') or 'CUSTOM').upper()

    if use_mode == 'KNOWN':
        known = (connector_cfg.get('known_type') or '').upper().strip()
        if not known:
            raise ValueError("known_type is required when connector.use == 'KNOWN'")
        try:
            return getattr(AtlanConnectorType, known)
        except AttributeError:
            available = [m for m in dir(AtlanConnectorType) if m.isupper() and not m.startswith('_')]
            logger.warning(f"KNOWN type '{known}' not found in SDK. Available: {available}. Falling back to CUSTOM.")
            # Convert to CUSTOM path using provided known value as custom name/value
            connector_cfg['use'] = 'CUSTOM'
            connector_cfg.setdefault('custom', {})
            connector_cfg['custom'].setdefault('name', known)
            connector_cfg['custom'].setdefault('value', known.lower())
            connector_cfg['custom'].setdefault('category', 'DATABASE')

    # Default to CUSTOM: register a custom connector type if provided
    custom = connector_cfg.get('custom', {})
    custom_name = (custom.get('name') or 'MY_CUSTOM').upper()
    custom_value = custom.get('value') or 'my-custom'
    category_str = (custom.get('category') or 'API').upper()

    try:
        category = getattr(AtlanConnectionCategory, category_str)
    except AttributeError:
        category = AtlanConnectionCategory.API

    try:
        create_custom = getattr(AtlanConnectorType, 'CREATE_CUSTOM')
        create_custom(name=custom_name, value=custom_value, category=category)
        return getattr(AtlanConnectorType, custom_name)
    except Exception:
        logger.warning("Falling back to API connector type; custom connector registration may be unsupported in this SDK version.")
        return AtlanConnectorType.API


def create_connection_and_assets(client: AtlanClient, config: Dict) -> bool:
    ccfg = config.get('custom_connection') or {}
    if not isinstance(ccfg, dict) or not ccfg.get('name'):
        logger.error("custom_connection.name is required in config.")
        return False

    connector_type = _get_connector_type(ccfg)
    admin_roles_cfg = ccfg.get('admin_roles') or ["$admin"]
    admin_role_guids = _resolve_admin_roles(client, admin_roles_cfg)

    connection_name = ccfg['name']
    logger.info(f"Creating connection '{connection_name}' of type '{connector_type.name}'...")

    connection = Connection.creator(
        client=client,
        name=connection_name,
        connector_type=connector_type,
        admin_roles=admin_role_guids if admin_role_guids else None,
    )

    resp = client.asset.save(connection)
    if not resp:
        logger.error("No response while saving the connection.")
        return False

    # Figure out qualified name of connection (from response or by re-query)
    connection_qn = None
    try:
        created = resp.assets_created(Connection) or []
        if created:
            connection_qn = created[0].qualified_name
    except Exception:
        pass

    # If not present in response, fetch via search
    if not connection_qn:
        try:
            from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
            res = client.asset.search(
                FluentSearch()
                .where(CompoundQuery.asset_type(Connection))
                .where(Connection.NAME.eq(connection_name))
                .page_size(1)
                .to_request()
            )
            for c in res:
                if isinstance(c, Connection) and c.name == connection_name:
                    logger.info(f"Connector detected for '{connection_name}': {getattr(c, 'connector_name', getattr(c, 'connector_type', 'Unknown'))}")
                    connection_qn = c.qualified_name
                    break
        except Exception:
            pass

    if not connection_qn:
        logger.error("Could not determine connection qualified name after creation.")
        return False

    logger.info(f"Connection ready: {connection_name} ({connection_qn})")

    assets_cfg = (ccfg.get('create_assets') or {}) if isinstance(ccfg, dict) else {}
    if not assets_cfg.get('enabled'):
        logger.info("Asset creation disabled. Done.")
        return True

    # Create Database
    db_name = assets_cfg.get('database')
    if not db_name:
        logger.warning("create_assets.enabled is true but no database name provided; skipping assets.")
        return True

    logger.info(f"Creating Database '{db_name}'...")
    db = Database.creator(name=db_name, connection_qualified_name=connection_qn)
    client.asset.save(db)

    # Create Schema
    schema_name = assets_cfg.get('schema') or 'PUBLIC'
    logger.info(f"Creating Schema '{schema_name}'...")
    sch = Schema.creator(name=schema_name, database_qualified_name=f"{connection_qn}/{db_name}")
    client.asset.save(sch)

    # Create Tables
    tables_cfg = assets_cfg.get('tables') or []
    for t in tables_cfg:
        t_name = t.get('name')
        if not t_name:
            continue
        table = Table.creator(
            name=t_name,
            schema_qualified_name=f"{connection_qn}/{db_name}/{schema_name}",
            description=t.get('description') or None,
        )
        client.asset.save(table)
        logger.info(f"Created table '{t_name}'.")

    logger.info("All requested assets created.")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a custom Atlan connection and optional assets")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        required=False,
        help="Path to YAML config file containing Atlan credentials and custom_connection settings",
    )
    args = parser.parse_args()

    config = _read_yaml_config(args.config)
    client = _initialize_client(config)
    if not client:
        return 1

    ok = create_connection_and_assets(client, config)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


