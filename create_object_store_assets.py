"""
Create object store assets (e.g., S3 buckets/objects) under an existing connection.

Config section used: object_store_assets

object_store_assets:
  enabled: true
  # Provide either connection_qualified_name OR connection_name
  connection_qualified_name: "default/s3/12345"
  connection_name: "S3-Prod"   # used if qualified name not provided
  provider: "s3"
  buckets:
    - name: "my-bucket"
      region: "us-east-1"
      objects:
        - key: "path/to/file.csv"
          name: "file.csv"
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
        logging.FileHandler(os.path.join(LOGS_DIR, 'object_store_assets_creation.log')),
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


def _provider_accepts_connector(connector_label: str) -> bool:
    label = (connector_label or '').lower()
    # Accept S3 and S3-compatible stores (ECS, MinIO, Wasabi, etc.)
    allowed = ['s3', 'ecs', 'minio', 'wasabi', 'digitalocean', 'api']
    return any(token in label for token in allowed)


def _s3_class_mapping() -> Dict[str, str]:
    return {
        'bucket_class': 'S3Bucket',
        'object_class': 'S3Object',
        'object_parent_param': 's3_bucket_qualified_name',
    }


def _build_creator_kwargs(creator, role: str, *,
                          name: str,
                          connection_qn: str,
                          bucket_name: Optional[str] = None,
                          parent_qn: Optional[str] = None,
                          aws_arn: Optional[str] = None,
                          key: Optional[str] = None) -> Dict[str, str]:
    """Build kwargs matching the creator signature across SDK variants."""
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

    # Bucket qualified name (parent)
    if parent_qn:
        for pqn in ('s3_bucket_qualified_name', 'bucket_qualified_name'):
            if pqn in params:
                kwargs[pqn] = parent_qn
                break

    # Bucket name
    if bucket_name:
        for bname in ('s3_bucket_name', 'bucket_name'):
            if bname in params:
                kwargs[bname] = bucket_name
                break

    # ARN
    if aws_arn:
        for arn in ('aws_arn', 'arn', 's3_arn', 'object_arn'):
            if arn in params:
                kwargs[arn] = aws_arn
                break

    # Key
    if key:
        for kk in ('s3_key', 'object_key', 'key', 's3_object_key'):
            if kk in params:
                kwargs[kk] = key
                break

    logger.debug(f"[{role}] creator accepts: {sorted(list(params)) if params else 'unknown'}, using kwargs: {kwargs}")
    return kwargs


def create_object_store_assets(client: AtlanClient, config: Dict) -> bool:
    ocfg = config.get('object_store_assets') or {}
    if not isinstance(ocfg, dict) or not ocfg.get('enabled'):
        logger.info("object_store_assets not enabled. Nothing to do.")
        return True

    # Enable verbose debug logging if requested
    if bool(ocfg.get('debug')):
        logger.setLevel(logging.DEBUG)
        for h in logger.handlers:
            try:
                h.setLevel(logging.DEBUG)
            except Exception:
                pass

    connection_qn = _resolve_connection_qn(client, ocfg)
    if not connection_qn:
        logger.error("Provide 'connection_qualified_name' or 'connection_name' under object_store_assets.")
        return False

    provider_raw = (ocfg.get('provider') or 's3')
    provider = str(provider_raw).lower()
    # Normalize common S3-compatible synonyms
    if provider in ['ecs', 'minio', 'wasabi', 'do', 'digitalocean']:
        provider = 's3'
    if provider != 's3':
        logger.error("Only S3-compatible object stores are supported. Set provider: 's3'.")
        return False
    assume_s3_compatible = bool(ocfg.get('assume_s3_compatible'))

    # Validate the connection exists and appears to be S3-compatible
    conn = _fetch_connection(client, connection_qn)
    if not conn:
        logger.error(
            "Connection not found for qualified name: %s. Ensure this connection exists and the QN is correct.",
            connection_qn,
        )
        return False
    connector_label = getattr(conn, 'connector_name', getattr(conn, 'connector_type', 'Unknown')) or 'Unknown'
    logger.debug(f"Detected connection '{getattr(conn, 'name', '?')}' type label: {connector_label}")
    if not _provider_accepts_connector(str(connector_label)):
        if assume_s3_compatible:
            logger.warning(
                "Proceeding under assume_s3_compatible=true: treating connection '%s' (type '%s') as S3-compatible.",
                conn.name,
                connector_label,
            )
        else:
            logger.error(
                "Provider mismatch: configured provider is 's3' but connection '%s' is of type '%s'. Set assume_s3_compatible=true to override.",
                conn.name,
                connector_label,
            )
            return False
    
    # Store connection name for OpenLineage namespace mapping documentation
    connection_name = conn.name

    try:
        from pyatlan import model as _pm  # type: ignore
        assets_module = _pm.assets if hasattr(_pm, 'assets') else None
        if not assets_module:
            from pyatlan.model import assets as assets_module  # fallback
        provider_map = _s3_class_mapping()
        bucket_class = getattr(assets_module, provider_map['bucket_class'])
        object_class = getattr(assets_module, provider_map['object_class'])
        object_parent_param = provider_map['object_parent_param']
    except Exception:
        logger.error(
            "This pyatlan version may not include S3Bucket/S3Object classes. Please upgrade pyatlan.",
        )
        return False

    buckets: List[Dict] = ocfg.get('buckets') or []
    if not buckets:
        logger.info("No buckets specified. Nothing to create.")
        return True
    
    # Log OpenLineage namespace mapping requirements
    first_bucket = buckets[0].get('name') if buckets else None
    if first_bucket:
        logger.warning("=" * 70)
        logger.warning("IMPORTANT: OpenLineage Namespace Mapping for Lineage")
        logger.warning("=" * 70)
        logger.warning(f"Your S3 connection name: {connection_name}")
        logger.warning(f"Your S3 bucket: {first_bucket}")
        logger.warning("")
        logger.warning("OpenLineage will send datasets with namespace: s3://%s", first_bucket)
        logger.warning("Atlan expects namespace: %s", connection_name)
        logger.warning("")
        logger.warning("For lineage to work, you need ONE of these solutions:")
        logger.warning("  1. Set Spark namespace to S3 connection name:")
        logger.warning("     openlineage_namespace = '%s'", connection_name)
        logger.warning("     (in spark_ol_minio.py)")
        logger.warning("")
        logger.warning("  2. Contact Atlan support to configure namespace mapping:")
        logger.warning("     s3://%s → %s", first_bucket, connection_name)
        logger.warning("=" * 70)

    for b in buckets:
        bucket_name = (b.get('name') or '').strip()
        if not bucket_name:
            continue
        bucket_arn = (b.get('aws_arn') or f"arn:aws:s3:::{bucket_name}").strip()
        logger.info(f"Creating S3 bucket asset '{bucket_name}'...")
        try:
            # Try introspected kwargs first
            b_kwargs = _build_creator_kwargs(
                bucket_class.creator,
                'bucket',
                name=bucket_name,
                connection_qn=connection_qn,
                bucket_name=bucket_name,
                parent_qn=f"{connection_qn}/{bucket_name}",
            )
            try:
                bucket = bucket_class.creator(**b_kwargs)
                client.asset.save(bucket)
            except Exception as te:
                logger.debug(f"Bucket creator rejected kwargs {b_kwargs.keys()}: {te}")
                # Fallback minimal
                bucket = bucket_class.creator(name=bucket_name, connection_qualified_name=connection_qn)
                client.asset.save(bucket)
        except Exception as e:
            logger.error(
                "Failed to create bucket '%s': %s. Hint: verify the connection qualified name belongs to an S3 connection (detected: %s).",
                bucket_name,
                e,
                connector_label,
            )
            continue

        for obj in b.get('objects') or []:
            key = (obj.get('key') or '').strip()
            if not key:
                continue
            obj_name = (obj.get('name') or key or os.path.basename(key)).strip()
            logger.info(f"Creating S3 object asset '{obj_name}' (key={key})...")
            try:
                parent_qn = f"{connection_qn}/{bucket_name}"
                key = key.strip('/')
                
                # Force obj_name to be basename of key to prevent ARN or full path leaking into qualified name construction
                obj_name = os.path.basename(key)
                
                # Construct ARN without the arn:aws:s3::: prefix to ensure correct qualified name
                # The qualified name should be: connection_qn/bucket_name/key
                obj_arn = f"{bucket_name}/{key}"
                
                o_kwargs = _build_creator_kwargs(
                    object_class.creator,
                    'object',
                    name=obj_name,
                    connection_qn=connection_qn,
                    bucket_name=bucket_name,
                    parent_qn=parent_qn,
                    aws_arn=obj_arn,
                    key=key,
                )
                candidate = object_class.creator(**o_kwargs)
                client.asset.save(candidate)
            except Exception as e:
                logger.error(f"Failed to create object '{obj_name}': {e}. Tried kwargs keys: {sorted(list(o_kwargs.keys())) if 'o_kwargs' in locals() else 'n/a'}")

    logger.info("Object store assets creation completed.")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Create object store assets (S3)")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        required=False,
        help="Path to YAML config file containing Atlan credentials and object_store_assets settings",
    )
    args = parser.parse_args()

    config = _read_yaml_config(args.config)
    client = _initialize_client(config)
    if not client:
        return 1

    ok = create_object_store_assets(client, config)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

