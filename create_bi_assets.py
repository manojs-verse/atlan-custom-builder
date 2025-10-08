"""
Create BI assets (e.g., Tableau projects/workbooks/dashboards) under an existing connection.

Config section used: bi_assets

bi_assets:
  enabled: true
  # Provide either connection_qualified_name OR connection_name
  connection_qualified_name: "default/tableau/12345"
  connection_name: "Tableau-Prod"   # used if qualified name not provided
  provider: "tableau"               # currently supported: tableau
  tableau:
    projects:
      - name: "Sales Analytics"
        workbooks:
          - name: "QBR 2025"
            dashboards:
              - name: "Executive Overview"
"""

import os
import logging
import argparse
from typing import Dict, Optional, List

import yaml
from dotenv import load_dotenv

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery


load_dotenv()

LOGS_DIR = os.path.join(os.getcwd(), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'bi_assets_creation.log')),
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


def create_bi_assets(client: AtlanClient, config: Dict) -> bool:
    bcfg = config.get('bi_assets') or {}
    if not isinstance(bcfg, dict) or not bcfg.get('enabled'):
        logger.info("bi_assets not enabled. Nothing to do.")
        return True

    connection_qn = _resolve_connection_qn(client, bcfg)
    if not connection_qn:
        logger.error("Provide 'connection_qualified_name' or 'connection_name' under bi_assets.")
        return False

    provider = (bcfg.get('provider') or 'tableau').lower()
    if provider != 'tableau':
        logger.error("Only provider 'tableau' is currently supported in this script.")
        return False

    try:
        from pyatlan.model.assets import TableauProject, TableauWorkbook, TableauDashboard
    except Exception:
        logger.error("This pyatlan version may not include Tableau asset classes. Please upgrade pyatlan.")
        return False

    tc = bcfg.get('tableau') or {}
    projects: List[Dict] = tc.get('projects') or []
    if not projects:
        logger.info("No Tableau projects specified. Nothing to create.
")
        return True

    for p in projects:
        pname = (p.get('name') or '').strip()
        if not pname:
            continue
        logger.info(f"Creating Tableau project '{pname}'...")
        try:
            project = TableauProject.creator(name=pname, connection_qualified_name=connection_qn)
            client.asset.save(project)
            project_qn = f"{connection_qn}/{pname}"
        except Exception as e:
            logger.error(f"Failed to create project '{pname}': {e}")
            continue

        for wb in p.get('workbooks') or []:
            wname = (wb.get('name') or '').strip()
            if not wname:
                continue
            logger.info(f"Creating Tableau workbook '{wname}'...")
            try:
                workbook = TableauWorkbook.creator(name=wname, project_qualified_name=project_qn)
                client.asset.save(workbook)
                workbook_qn = f"{project_qn}/{wname}"
            except Exception as e:
                logger.error(f"Failed to create workbook '{wname}': {e}")
                continue

            for d in wb.get('dashboards') or []:
                dname = (d.get('name') or '').strip()
                if not dname:
                    continue
                logger.info(f"Creating Tableau dashboard '{dname}'...")
                try:
                    dashboard = TableauDashboard.creator(name=dname, workbook_qualified_name=workbook_qn)
                    client.asset.save(dashboard)
                except Exception as e:
                    logger.error(f"Failed to create dashboard '{dname}': {e}")

    logger.info("BI assets creation completed.")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Create BI assets (Tableau)")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        required=False,
        help="Path to YAML config file containing Atlan credentials and bi_assets settings",
    )
    args = parser.parse_args()

    config = _read_yaml_config(args.config)
    client = _initialize_client(config)
    if not client:
        return 1

    ok = create_bi_assets(client, config)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


