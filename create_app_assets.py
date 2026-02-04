"""
Create App assets (Application and optional ApplicationField) under an existing connection.

Config section used: app_assets

app_assets:
  enabled: true
  # Provide either connection_qualified_name OR connection_name
  connection_qualified_name: "default/app/12345"
  connection_name: "My-App-Connection"   # used if qualified name not provided
  applications:
    - name: "my-application"
      description: "My custom application"
      fields:
        - name: "application-field"
          description: "A field in the application"
          owned_assets:
            - qualified_name: "default/snowflake/123456789/DATABASE/SCHEMA/TABLE"
"""

import os
import logging
import argparse
import inspect
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
        logging.FileHandler(os.path.join(LOGS_DIR, 'app_assets_creation.log')),
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
    logger.error(f"Could not resolve connection by name: {name}")
    return None


def _fetch_connection(client: AtlanClient, connection_qn: str) -> Optional[Connection]:
    """Fetch a connection by qualified name."""
    try:
        request = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Connection))
            .where(Connection.QUALIFIED_NAME.eq(connection_qn))
            .page_size(1)
            .to_request()
        )
        response = client.asset.search(request)
        for c in response:
            if isinstance(c, Connection) and c.qualified_name == connection_qn:
                return c
    except Exception as e:
        logger.error(f"Failed to fetch connection by qualified name '{connection_qn}': {e}")
    return None


def _build_application_creator_kwargs(creator, name: str, connection_qn: str) -> Dict[str, str]:
    """Build kwargs matching the Application creator signature across SDK variants."""
    try:
        params = set(inspect.signature(creator).parameters.keys())
    except Exception:
        params = set()

    kwargs: Dict[str, str] = {}
    # Always provide name if accepted
    if 'name' in params or not params:
        kwargs['name'] = name

    # Connection QN
    for cqn in ('connection_qualified_name', 'connectionQN', 'connection_qn'):
        if cqn in params:
            kwargs[cqn] = connection_qn
            break

    logger.debug(f"[Application] creator accepts: {sorted(list(params)) if params else 'unknown'}, using kwargs: {kwargs}")
    return kwargs


def _build_application_field_creator_kwargs(creator, name: str, application_qn: str) -> Dict[str, str]:
    """Build kwargs matching the ApplicationField creator signature across SDK variants."""
    try:
        params = set(inspect.signature(creator).parameters.keys())
    except Exception:
        params = set()

    kwargs: Dict[str, str] = {}
    # Always provide name if accepted
    if 'name' in params or not params:
        kwargs['name'] = name

    # Application QN - can be passed as application_qualified_name or application
    for aqn in ('application_qualified_name', 'applicationQN', 'application_qn', 'application'):
        if aqn in params:
            if aqn == 'application':
                # If it expects an Application object, we'll need to fetch it
                # For now, try passing the QN string
                kwargs[aqn] = application_qn
            else:
                kwargs[aqn] = application_qn
            break

    logger.debug(f"[ApplicationField] creator accepts: {sorted(list(params)) if params else 'unknown'}, using kwargs: {kwargs}")
    return kwargs


def create_app_assets(client: AtlanClient, config: Dict) -> bool:
    acfg = config.get('app_assets') or {}
    if not isinstance(acfg, dict) or not acfg.get('enabled'):
        logger.info("app_assets not enabled. Nothing to do.")
        return True

    # Enable verbose debug logging if requested
    if bool(acfg.get('debug')):
        logger.setLevel(logging.DEBUG)
        for h in logger.handlers:
            try:
                h.setLevel(logging.DEBUG)
            except Exception:
                pass

    connection_qn = _resolve_connection_qn(client, acfg)
    if not connection_qn:
        logger.error("Provide 'connection_qualified_name' or 'connection_name' under app_assets.")
        return False

    # Validate the connection exists and appears to be an App connection
    conn = _fetch_connection(client, connection_qn)
    if not conn:
        logger.error(
            "Connection not found for qualified name: %s. Ensure this connection exists and the QN is correct.",
            connection_qn,
        )
        return False
    connector_label = getattr(conn, 'connector_name', getattr(conn, 'connector_type', 'Unknown')) or 'Unknown'
    logger.debug(f"Detected connection '{getattr(conn, 'name', '?')}' type label: {connector_label}")
    
    # Check if it's an app connection
    if str(connector_label).lower() != 'app':
        logger.warning(
            "Connection '%s' is of type '%s', expected 'app'. Proceeding anyway...",
            conn.name,
            connector_label,
        )

    try:
        from pyatlan import model as _pm  # type: ignore
        assets_module = _pm.assets if hasattr(_pm, 'assets') else None
        if not assets_module:
            from pyatlan.model import assets as assets_module  # fallback
        Application = getattr(assets_module, 'Application')
        ApplicationField = getattr(assets_module, 'ApplicationField')
    except AttributeError as e:
        logger.error(
            "This pyatlan version may not include Application/ApplicationField classes. Please upgrade pyatlan. Error: %s",
            e,
        )
        return False
    except Exception as e:
        logger.error(f"Failed to import Application classes: {e}")
        return False
    
    # Store assets_module for use in owned assets resolution
    _assets_module = assets_module

    applications: List[Dict] = acfg.get('applications') or []
    if not applications:
        logger.info("No applications specified. Nothing to create.")
        return True

    for app_cfg in applications:
        app_name = (app_cfg.get('name') or '').strip()
        if not app_name:
            logger.warning("Skipping application with no name.")
            continue

        app_description = app_cfg.get('description') or None
        logger.info(f"Creating Application asset '{app_name}'...")
        
        try:
            # Try introspected kwargs first
            app_kwargs = _build_application_creator_kwargs(
                Application.creator,
                name=app_name,
                connection_qn=connection_qn,
            )
            try:
                application = Application.creator(**app_kwargs)
                if app_description:
                    application.description = app_description
                resp = client.asset.save(application)
                
                # Get the qualified name from response
                application_qn = None
                try:
                    created = resp.assets_created(Application) or []
                    if created:
                        application_qn = created[0].qualified_name
                        logger.info(f"Created Application '{app_name}' with qualified name: {application_qn}")
                except Exception:
                    # Fallback: construct expected QN pattern
                    # Pattern: default/app/<epoch>/<applicationName>
                    # We'll need to search for it
                    pass
                
                # If not in response, search for it
                if not application_qn:
                    try:
                        search_req = (
                            FluentSearch()
                            .where(CompoundQuery.asset_type(Application))
                            .where(Application.NAME.eq(app_name))
                            .where(Application.CONNECTION_QUALIFIED_NAME.eq(connection_qn))
                            .page_size(1)
                            .to_request()
                        )
                        search_resp = client.asset.search(search_req)
                        for app in search_resp:
                            if isinstance(app, Application) and app.name == app_name:
                                application_qn = app.qualified_name
                                logger.info(f"Found Application '{app_name}' with qualified name: {application_qn}")
                                break
                    except Exception as e:
                        logger.warning(f"Could not determine application qualified name: {e}")
                
            except Exception as te:
                logger.debug(f"Application creator rejected kwargs {app_kwargs.keys()}: {te}")
                # Fallback minimal
                application = Application.creator(name=app_name, connection_qualified_name=connection_qn)
                if app_description:
                    application.description = app_description
                resp = client.asset.save(application)
                # Try to get QN from response
                application_qn = None
                try:
                    created = resp.assets_created(Application) or []
                    if created:
                        application_qn = created[0].qualified_name
                except Exception:
                    pass
                
        except Exception as e:
            logger.error(
                "Failed to create application '%s': %s. Hint: verify the connection qualified name belongs to an App connection (detected: %s).",
                app_name,
                e,
                connector_label,
            )
            continue

        # Create ApplicationFields if specified
        fields_cfg = app_cfg.get('fields') or []
        if fields_cfg and not application_qn:
            logger.warning(f"Cannot create fields for '{app_name}' - could not determine application qualified name.")
            continue

        for field_cfg in fields_cfg:
            field_name = (field_cfg.get('name') or '').strip()
            if not field_name:
                continue

            field_description = field_cfg.get('description') or None
            owned_assets_qns = field_cfg.get('owned_assets') or []
            
            logger.info(f"Creating ApplicationField '{field_name}' for application '{app_name}'...")
            
            try:
                field_kwargs = _build_application_field_creator_kwargs(
                    ApplicationField.creator,
                    name=field_name,
                    application_qn=application_qn,
                )
                
                try:
                    application_field = ApplicationField.creator(**field_kwargs)
                    if field_description:
                        application_field.description = field_description
                    
                    # Add owned assets if specified
                    if owned_assets_qns:
                        try:
                            # Try to set owned assets
                            # The owned assets should be Asset references
                            # We need to search for each asset and create a reference
                            from pyatlan.model.assets import Asset
                            from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
                            
                            owned_refs = []
                            for asset_qn in owned_assets_qns:
                                try:
                                    # Try to find the asset by qualified name
                                    # We'll search across common asset types
                                    asset_found = False
                                    for asset_type_name in ['Table', 'Schema', 'Database', 'View', 'Column', 'S3Object', 'S3Bucket']:
                                        try:
                                            asset_class = getattr(_assets_module, asset_type_name, None)
                                            if asset_class:
                                                search_req = (
                                                    FluentSearch()
                                                    .where(CompoundQuery.asset_type(asset_class))
                                                    .where(asset_class.QUALIFIED_NAME.eq(asset_qn))
                                                    .page_size(1)
                                                    .to_request()
                                                )
                                                search_resp = client.asset.search(search_req)
                                                for found_asset in search_resp:
                                                    if isinstance(found_asset, asset_class) and found_asset.qualified_name == asset_qn:
                                                        # Create reference using ref_by_guid
                                                        if hasattr(asset_class, 'ref_by_guid'):
                                                            ref = asset_class.ref_by_guid(guid=found_asset.guid)
                                                            owned_refs.append(ref)
                                                            asset_found = True
                                                            break
                                                if asset_found:
                                                    break
                                        except Exception:
                                            continue
                                    
                                    if not asset_found:
                                        logger.warning(f"Could not find asset with qualified name: {asset_qn}")
                                except Exception as e:
                                    logger.warning(f"Failed to create reference for asset {asset_qn}: {e}")
                            
                            if owned_refs:
                                # Set the owned assets attribute
                                if hasattr(application_field, 'application_field_owned_assets'):
                                    application_field.application_field_owned_assets = owned_refs
                                elif hasattr(application_field, 'applicationFieldOwnedAssets'):
                                    application_field.applicationFieldOwnedAssets = owned_refs
                                logger.info(f"Linked {len(owned_refs)} asset(s) to ApplicationField '{field_name}'")
                        except Exception as e:
                            logger.warning(f"Could not set owned assets for field '{field_name}': {e}")
                    
                    client.asset.save(application_field)
                    logger.info(f"Created ApplicationField '{field_name}'.")
                    
                except Exception as te:
                    logger.debug(f"ApplicationField creator rejected kwargs {field_kwargs.keys()}: {te}")
                    # Fallback: try with application_qualified_name directly
                    try:
                        application_field = ApplicationField.creator(
                            name=field_name,
                            application_qualified_name=application_qn
                        )
                        if field_description:
                            application_field.description = field_description
                        client.asset.save(application_field)
                        logger.info(f"Created ApplicationField '{field_name}' (fallback method).")
                    except Exception as e2:
                        logger.error(f"Failed to create ApplicationField '{field_name}' even with fallback: {e2}")
                        
            except Exception as e:
                logger.error(f"Failed to create ApplicationField '{field_name}': {e}")

    logger.info("App assets creation completed.")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Create App assets (Application and ApplicationField)")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        required=False,
        help="Path to YAML config file containing Atlan credentials and app_assets settings",
    )
    args = parser.parse_args()

    config = _read_yaml_config(args.config)
    client = _initialize_client(config)
    if not client:
        return 1

    ok = create_app_assets(client, config)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
