"""
Interactive Lineage Creator for Atlan
Creates lineage between assets by searching for them interactively
Based on: https://developer.atlan.com/snippets/common-examples/lineage/manage/#directly
"""

import os
import logging
from dotenv import load_dotenv
import argparse
import yaml
from typing import List, Dict, Optional

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Process, Table, Column, View, Connection
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery

load_dotenv()

LOGS_DIR = os.path.join(os.getcwd(), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'lineage_creation.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class InteractiveLineageCreator:
    """Creates lineage between assets interactively"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.client = None
        self.config = config or {}
        self.supported_asset_types = {
            "1": {"name": "Table", "class": Table, "description": "Database tables"},
            "2": {"name": "View", "class": View, "description": "Database views"},
            "3": {"name": "Column", "class": Column, "description": "Table/view columns"}
        }

    @staticmethod
    def _derive_connection_qn_from_asset_qn(asset_qualified_name: Optional[str]) -> Optional[str]:
        """Derive connection qualified name from an asset qualified name by taking the first 3 segments."""
        if not asset_qualified_name or not isinstance(asset_qualified_name, str):
            return None
        parts = asset_qualified_name.split('/')
        if len(parts) >= 3:
            return '/'.join(parts[:3])
        return None
        
    def _extract_atlan_credentials(self) -> Dict[str, Optional[str]]:
        """Extract BASE_URL and API_KEY from config (supports flat or nested under 'atlan')."""
        base_url = None
        api_key = None

        # Support nested structure: { atlan: { base_url: ..., api_key: ... } }
        atlan_cfg = self.config.get("atlan") if isinstance(self.config, dict) else None
        if isinstance(atlan_cfg, dict):
            base_url = atlan_cfg.get("base_url") or atlan_cfg.get("BASE_URL")
            api_key = atlan_cfg.get("api_key") or atlan_cfg.get("API_KEY")

        # Fallback to flat keys at root
        if not base_url:
            base_url = self.config.get("base_url") or self.config.get("BASE_URL")
        if not api_key:
            api_key = self.config.get("api_key") or self.config.get("API_KEY")

        return {"base_url": base_url, "api_key": api_key}

    def initialize_client(self) -> bool:
        """Initialize the Atlan client"""
        try:
            creds_from_cfg = self._extract_atlan_credentials()
            api_key = creds_from_cfg.get("api_key") or os.getenv("API_KEY")
            base_url = creds_from_cfg.get("base_url") or os.getenv("BASE_URL")
            
            if not api_key or not base_url:
                logger.error("API_KEY and BASE_URL must be set in environment variables")
                return False
                
            self.client = AtlanClient(api_key=api_key, base_url=base_url)
            logger.info("✅ Atlan client initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Atlan client: {e}")
            return False

    def search_connections(self, search_term: str = "", limit: int = 20) -> List[Dict]:
        """Search for connections using CompoundQuery"""
        try:
            request = (
                FluentSearch()
                .where(CompoundQuery.asset_type(Connection))
                .where(CompoundQuery.active_assets())
                .page_size(limit)
                .to_request()
            )
            
            response = self.client.asset.search(request)
            
            connections = []
            for conn in response:
                if isinstance(conn, Connection):
                    if search_term and search_term.lower() not in conn.name.lower():
                        continue
                        
                    connections.append({
                        'guid': conn.guid,
                        'qualified_name': conn.qualified_name,
                        'name': conn.name,
                        'type': conn.type_name,
                        'connector_type': getattr(conn, 'connector_name', getattr(conn, 'connector_type', 'Unknown'))
                    })
            
            return connections
            
        except Exception as e:
            logger.error(f"❌ Failed to search for connections: {e}")
            return []

    def display_connections(self, connections: List[Dict]):
        """Display available connections"""
        if not connections:
            print("❌ No connections found.")
            return
        
        print(f"\n Found {len(connections)} connection(s):")
        print("=" * 80)
        for i, conn in enumerate(connections, 1):
            print(f"{i}. {conn['name']}")
            print(f"   Type: {conn['connector_type']}")
            print(f"   Qualified Name: {conn['qualified_name']}")
            print("-" * 80)

    def select_connection_for_step(self, step_name: str) -> Optional[Dict]:
        """Let user select a connection for a specific step"""
        print(f"\n {step_name}: Select Connection")
        print("=" * 50)
        
        search_term = input(" Enter connection name (or press Enter to see all): ").strip()
        
        print(" Searching for connections...")
        connections = self.search_connections(search_term)
        
        if not connections:
            print("❌ No connections found")
            return None
        
        self.display_connections(connections)
        
        while True:
            try:
                choice = input(f"\nSelect a connection (1-{len(connections)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    return None
                
                index = int(choice) - 1
                if 0 <= index < len(connections):
                    selected = connections[index]
                    print(f"✅ Selected connection: {selected['name']}")
                    return selected
                else:
                    print(f"❌ Please enter a number between 1 and {len(connections)}")
                    
            except ValueError:
                print("❌ Please enter a valid number or 'q' to quit")

    def display_asset_types(self):
        """Display supported asset types"""
        print("\n Supported Asset Types:")
        print("=" * 50)
        for key, asset_info in self.supported_asset_types.items():
            print(f"{key}. {asset_info['name']} - {asset_info['description']}")
        print("=" * 50)

    def get_asset_details_from_user(self, asset_type_name: str) -> Dict:
        """Get specific asset details from user to make targeted search"""
        print(f"\n To find {asset_type_name} efficiently, please provide details:")
        print(" TIP: Use exact case (UPPERCASE/lowercase) as it appears in Atlan")
        
        details = {}
        
        database = input(" Database name (e.g., 'WIDE_WORLD_IMPORTERS', 'ATLAN_TESTING_DB'): ").strip()
        if database:
            details['database'] = database
        
        schema = input(" Schema name (e.g., 'APPLICATION', 'PUBLIC', 'sales'): ").strip()
        if schema:
            details['schema'] = schema
        
        asset_name = input(f" {asset_type_name} name (exact or partial): ").strip()
        if asset_name:
            details['name'] = asset_name
        
        return details

    def search_by_qualified_name_direct(self, asset_type_class, qualified_name: str) -> List[Dict]:
        """Direct search by qualified name"""
        try:
            print(f" Direct search for: {qualified_name}")
            
            request = (
                FluentSearch()
                .where(CompoundQuery.asset_type(asset_type_class))
                .where(asset_type_class.QUALIFIED_NAME.eq(qualified_name))
                .page_size(1)
                .to_request()
            )
            
            response = self.client.asset.search(request)
            
            assets = []
            for asset in response:
                if isinstance(asset, asset_type_class):
                    assets.append({
                        'guid': asset.guid,
                        'qualified_name': asset.qualified_name,
                        'name': asset.name,
                        'type': asset.type_name,
                        'connection_name': getattr(asset, 'connection_name', 'Unknown'),
                        'connection_qualified_name': getattr(asset, 'connection_qualified_name', None),
                        'database_name': getattr(asset, 'database_name', 'Unknown'),
                        'schema_name': getattr(asset, 'schema_name', 'Unknown')
                    })
            
            return assets
            
        except Exception as e:
            logger.error(f"❌ Failed direct search: {e}")
            return []

    def search_by_qualified_name_prefix(self, asset_type_class, qn_prefix: str, limit: int = 5) -> List[Dict]:
        """Fallback: search by qualified name prefix when exact match fails."""
        try:
            print(f" Prefix search for: {qn_prefix}*")
            request = (
                FluentSearch()
                .where(CompoundQuery.asset_type(asset_type_class))
                .where(CompoundQuery.active_assets())
                .where(asset_type_class.QUALIFIED_NAME.startswith(qn_prefix))
                .page_size(limit)
                .to_request()
            )
            response = self.client.asset.search(request)
            assets = []
            for asset in response:
                if isinstance(asset, asset_type_class):
                    assets.append({
                        'guid': asset.guid,
                        'qualified_name': asset.qualified_name,
                        'name': asset.name,
                        'type': asset.type_name,
                        'connection_name': getattr(asset, 'connection_name', 'Unknown'),
                        'connection_qualified_name': getattr(asset, 'connection_qualified_name', None),
                        'database_name': getattr(asset, 'database_name', 'Unknown'),
                        'schema_name': getattr(asset, 'schema_name', 'Unknown')
                    })
            return assets
        except Exception as e:
            logger.error(f"❌ Failed prefix search: {e}")
            return []

    def search_assets_targeted_for_connection(self, asset_type_class, details: Dict, connection: Dict) -> List[Dict]:
        """Targeted search for a specific connection"""
        try:
            connection_qn = connection['qualified_name']
            conn_parts = connection_qn.split('/')
            
            if len(conn_parts) >= 3:
                base_qn = '/'.join(conn_parts[:3])
                
                if details.get('database') and details.get('schema'):
                    search_pattern = f"{base_qn}/{details['database']}/{details['schema']}"
                    page_size = 50
                elif details.get('database'):
                    search_pattern = f"{base_qn}/{details['database']}"
                    page_size = 100
                else:
                    search_pattern = base_qn
                    page_size = 200
                
                print(f" Searching with pattern: {search_pattern}/*")
                
                request = (
                    FluentSearch()
                    .where(CompoundQuery.asset_type(asset_type_class))
                    .where(CompoundQuery.active_assets())
                    .where(asset_type_class.QUALIFIED_NAME.startswith(search_pattern))
                    .page_size(page_size)
                    .to_request()
                )
            else:
                print("❌ Could not parse connection qualified name")
                return []
            
            print(" Executing targeted search...")
            response = self.client.asset.search(request)
            
            assets = []
            for asset in response:
                if not isinstance(asset, asset_type_class):
                    continue
                
                if details.get('name'):
                    if details['name'].lower() not in asset.name.lower():
                        continue
                
                assets.append({
                    'guid': asset.guid,
                    'qualified_name': asset.qualified_name,
                    'name': asset.name,
                    'type': asset.type_name,
                    'connection_name': getattr(asset, 'connection_name', 'Unknown'),
                    'database_name': getattr(asset, 'database_name', 'Unknown'),
                    'schema_name': getattr(asset, 'schema_name', 'Unknown')
                })
            
            print(f"✅ Found {len(assets)} matching assets")
            return assets
            
        except Exception as e:
            logger.error(f"❌ Failed targeted search: {e}")
            return []

    def search_assets_in_connection_specific(self, asset_type_class, search_term: str, connection: Dict, limit: int = 20) -> List[Dict]:
        """Search assets in a specific connection"""
        try:
            request = (
                FluentSearch()
                .where(CompoundQuery.asset_type(asset_type_class))
                .where(CompoundQuery.active_assets())
                .page_size(limit * 5)
                .to_request()
            )
            
            response = self.client.asset.search(request)
            
            assets = []
            count = 0
            for asset in response:
                if count >= limit:
                    break
                    
                if not isinstance(asset, asset_type_class):
                    continue
                    
                asset_connection_qn = getattr(asset, 'connection_qualified_name', '')
                if asset_connection_qn != connection['qualified_name']:
                    continue
                
                if search_term and search_term.lower() not in asset.name.lower():
                    continue
                    
                assets.append({
                    'guid': asset.guid,
                    'qualified_name': asset.qualified_name,
                    'name': asset.name,
                    'type': asset.type_name,
                    'connection_name': getattr(asset, 'connection_name', 'Unknown'),
                    'database_name': getattr(asset, 'database_name', 'Unknown'),
                    'schema_name': getattr(asset, 'schema_name', 'Unknown')
                })
                count += 1
            
            return assets
            
        except Exception as e:
            logger.error(f"❌ Failed connection-specific search: {e}")
            return []

    def display_search_results(self, assets: List[Dict], asset_type: str):
        """Display search results"""
        if not assets:
            print(f"❌ No {asset_type} assets found.")
            return
        
        print(f"\n Found {len(assets)} {asset_type} asset(s):")
        print("=" * 100)
        for i, asset in enumerate(assets, 1):
            print(f"{i}. {asset['name']}")
            print(f"   Type: {asset['type']}")
            print(f"   Database: {asset['database_name']}")
            print(f"   Schema: {asset['schema_name']}")
            print(f"   Qualified Name: {asset['qualified_name']}")
            print("-" * 100)

    def get_user_asset_selection(self, assets: List[Dict]) -> Optional[Dict]:
        """Get user's asset selection"""
        while True:
            try:
                choice = input(f"\nSelect an asset (1-{len(assets)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    return None
                
                index = int(choice) - 1
                if 0 <= index < len(assets):
                    return assets[index]
                else:
                    print(f"❌ Please enter a number between 1 and {len(assets)}")
                    
            except ValueError:
                print("❌ Please enter a valid number or 'q' to quit")

    def select_assets_interactively(self, role: str) -> List[Dict]:
        """Interactively select assets for input or output with connection selection"""
        selected_assets = []
        
        print(f"\n STEP: Select {role.upper()} assets for lineage...")
        
        step_connection = self.select_connection_for_step(f"STEP - {role.upper()} Assets")
        if not step_connection:
            print(f"❌ No connection selected for {role} assets. Cannot proceed.")
            return []
        
        current_connection = step_connection
        print(f"\n Selecting {role} assets from '{current_connection['name']}'...")
        
        while True:
            self.display_asset_types()
            asset_type_choice = input(f"\nSelect asset type for {role} (1-{len(self.supported_asset_types)}) or 'done' to finish: ").strip()
            
            if asset_type_choice.lower() == 'done':
                break
                
            if asset_type_choice not in self.supported_asset_types:
                print("❌ Invalid asset type selection")
                continue
            
            asset_info = self.supported_asset_types[asset_type_choice]
            asset_class = asset_info['class']
            asset_type_name = asset_info['name']
            
            print(f"\n How do you want to search for {asset_type_name}?")
            print("1.  Direct qualified name (fastest - if you know it)")
            print("2.  Provide details (database, schema, name)")
            print("3.  Simple name search (slower)")
            
            search_method = input("Choose search method (1-3): ").strip()
            
            found_assets = []
            
            if search_method == "1":
                qualified_name = input(f"Enter exact qualified name for {asset_type_name}: ").strip()
                if qualified_name:
                    found_assets = self.search_by_qualified_name_direct(asset_class, qualified_name)
            
            elif search_method == "2":
                details = self.get_asset_details_from_user(asset_type_name)
                if details:
                    found_assets = self.search_assets_targeted_for_connection(asset_class, details, current_connection)
            
            elif search_method == "3":
                search_term = input(f"Enter {asset_type_name} name (partial): ").strip()
                if search_term:
                    print("⚠️  This might be slow...")
                    found_assets = self.search_assets_in_connection_specific(asset_class, search_term, current_connection, limit=20)
            
            else:
                print("❌ Invalid search method")
                continue
            
            if not found_assets:
                print(f"❌ No {asset_type_name} assets found")
                retry = input("Try a different search? (y/n): ").strip().lower()
                if retry == 'y':
                    continue
                else:
                    break
            
            self.display_search_results(found_assets, asset_type_name)
            
            selected_asset = self.get_user_asset_selection(found_assets)
            
            if selected_asset:
                selected_assets.append({
                    **selected_asset,
                    'asset_class': asset_class,
                    'connection_info': current_connection
                })
                print(f"✅ Added {selected_asset['name']} from {current_connection['name']} to {role} assets")
            
            add_more = input(f"\nAdd another {role} asset? (y/n): ").strip().lower()
            if add_more != 'y':
                break
        
        return selected_assets

    def _defaults_section(self) -> Dict:
        defaults = self.config.get('defaults', {}) if isinstance(self.config, dict) else {}
        return defaults if isinstance(defaults, dict) else {}

    def _find_asset_by_qn_any(self, qualified_name: str) -> Optional[Dict]:
        """Try to resolve a qualified name against supported asset classes.
        Returns a selected asset dict compatible with downstream usage (includes asset_class and connection_info)."""
        for asset_info in self.supported_asset_types.values():
            asset_class = asset_info['class']
            results = self.search_by_qualified_name_direct(asset_class, qualified_name)
            if results:
                found = results[0]
                derived_conn_qn = (
                    found.get('connection_qualified_name')
                    or self._derive_connection_qn_from_asset_qn(found.get('qualified_name'))
                    or ''
                )
                connection_info = {
                    'qualified_name': derived_conn_qn,
                    'name': found.get('connection_name') or 'Unknown'
                }
                return {
                    **found,
                    'asset_class': asset_class,
                    'connection_info': connection_info
                }
            # Fallback to prefix search if not an exact match
            prefix_results = self.search_by_qualified_name_prefix(asset_class, qualified_name, limit=5)
            if prefix_results:
                found = prefix_results[0]
                derived_conn_qn = (
                    found.get('connection_qualified_name')
                    or self._derive_connection_qn_from_asset_qn(found.get('qualified_name'))
                    or ''
                )
                connection_info = {
                    'qualified_name': derived_conn_qn,
                    'name': found.get('connection_name') or 'Unknown'
                }
                return {
                    **found,
                    'asset_class': asset_class,
                    'connection_info': connection_info
                }
        return None

    def _assets_from_defaults(self, role: str) -> List[Dict]:
        """Build assets from defaults[inputs]/[outputs] if present. Values are qualified names (strings)."""
        defaults = self._defaults_section()
        items = defaults.get('inputs' if role == 'input' else 'outputs')
        if not items or not isinstance(items, list):
            return []
        selected: List[Dict] = []
        for qn in items:
            if not isinstance(qn, str) or not qn.strip():
                continue
            resolved = self._find_asset_by_qn_any(qn.strip())
            if resolved:
                selected.append(resolved)
            else:
                logger.warning(f"Could not resolve {role} asset by qualified name: {qn}")
        return selected

    def create_process_name(self, inputs: List[Dict], outputs: List[Dict]) -> str:
        """Create a descriptive process name"""
        input_names = [asset['name'] for asset in inputs[:3]]
        output_names = [asset['name'] for asset in outputs[:3]]
        
        input_str = ', '.join(input_names)
        if len(inputs) > 3:
            input_str += f" (+{len(inputs) - 3} more)"
        
        output_str = ', '.join(output_names)
        if len(outputs) > 3:
            output_str += f" (+{len(outputs) - 3} more)"
        
        return f"{input_str} -> {output_str}"

    def create_lineage_process(self, inputs: List[Dict], outputs: List[Dict]) -> bool:
        """Create the lineage process"""
        try:
            defaults = self._defaults_section()
            # Prefer explicit override from config if provided
            override_conn_qn = None
            if isinstance(defaults.get('process_connection_qualified_name'), str):
                override_conn_qn = defaults.get('process_connection_qualified_name').strip() or None

            process_connection = None
            if inputs:
                process_connection = inputs[0].get('connection_info')
            elif outputs:
                process_connection = outputs[0].get('connection_info')

            connection_qn = None
            if override_conn_qn:
                connection_qn = override_conn_qn
            elif process_connection and process_connection.get('qualified_name'):
                connection_qn = process_connection['qualified_name']
            else:
                # Derive from first available asset qualified name
                asset_qn = None
                if inputs:
                    asset_qn = inputs[0].get('qualified_name')
                elif outputs:
                    asset_qn = outputs[0].get('qualified_name')
                connection_qn = self._derive_connection_qn_from_asset_qn(asset_qn)

            if not connection_qn:
                print("❌ No connection information available")
                return False
            
            process_name = self.create_process_name(inputs, outputs)
            
            print(f"\n Process Details:")
            print(f"   Name: {process_name}")
            print(f"   Process Connection: {process_connection['name'] if process_connection else 'Unknown'}")
            
            defaults = self._defaults_section()
            default_process_id = (defaults.get('process_id') or '').strip() if isinstance(defaults.get('process_id'), str) else ''
            default_sql = (defaults.get('sql') or '').strip() if isinstance(defaults.get('sql'), str) else ''
            default_source_url = (defaults.get('source_url') or '').strip() if isinstance(defaults.get('source_url'), str) else ''
            auto_confirm = bool(defaults.get('auto_confirm'))

            if auto_confirm:
                process_id = default_process_id or f"lineage_process_{len(inputs)}_{len(outputs)}"
                sql_query = default_sql
                source_url = default_source_url
            else:
                process_id = input("\n🔧 Enter Process ID (unique identifier, e.g., 'etl_job_123'): ").strip() or default_process_id
                if not process_id:
                    process_id = f"lineage_process_{len(inputs)}_{len(outputs)}"
                sql_query = input("🔧 Enter SQL query (optional, press Enter to skip): ").strip() or default_sql
                source_url = input("🔧 Enter source URL (optional, press Enter to skip): ").strip() or default_source_url
            
            input_refs = []
            for asset in inputs:
                asset_class = asset['asset_class']
                input_refs.append(asset_class.ref_by_guid(guid=asset['guid']))
            
            output_refs = []
            for asset in outputs:
                asset_class = asset['asset_class']
                output_refs.append(asset_class.ref_by_guid(guid=asset['guid']))
            
            process = Process.creator(
                name=process_name,
                connection_qualified_name=connection_qn,
                process_id=process_id,
                inputs=input_refs,
                outputs=output_refs,
            )
            
            if sql_query:
                process.sql = sql_query
            
            if source_url:
                process.source_url = source_url
            
            logger.info(" Creating lineage process...")
            response = self.client.asset.save(process)
            
            if response:
                created_processes = response.assets_created(Process) or []
                updated_processes = response.assets_updated(Process) or []
                if created_processes or updated_processes:
                    if created_processes:
                        logger.info(f"✅ Created {len(created_processes)} lineage process(es)")
                    if updated_processes:
                        logger.info(f"🛠️ Updated {len(updated_processes)} lineage process(es)")
                    return True
                # Some environments return only guid_assignments for creations
                try:
                    guid_map = getattr(response, 'guid_assignments', None) or {}
                    if isinstance(guid_map, dict) and len(guid_map) > 0:
                        logger.info(f"✅ Lineage process persisted (guid assignments: {guid_map})")
                        return True
                except Exception:
                    pass
            logger.error("❌ No lineage process was created or updated")
            try:
                logger.error(f" Response payload: {response}")
            except Exception:
                pass
            return False
                
        except Exception as e:
            logger.error(f"❌ Failed to create lineage: {e}")
            return False

    def run(self) -> bool:
        """Main execution method"""
       
        if not self.initialize_client():
            return False
        
        defaults = self._defaults_section()
        inputs = self._assets_from_defaults("input")
        if not inputs:
            print("\n STEP 1: Select INPUT assets (sources)")
            inputs = self.select_assets_interactively("input")
        
        if not inputs:
            print("❌ No input assets selected. Lineage requires at least one input.")
            return False
        
        outputs = self._assets_from_defaults("output")
        if not outputs:
            print("\n STEP 2: Select OUTPUT assets (targets)")
            outputs = self.select_assets_interactively("output")
        
        if not outputs:
            print("❌ No output assets selected. Lineage requires at least one output.")
            return False
        
        print("\n LINEAGE SUMMARY:")
        print("=" * 50)
        
        print(f" Inputs ({len(inputs)}):")
        for asset in inputs:
            conn_name = asset.get('connection_info', {}).get('name', 'Unknown')
            print(f"   - {asset['name']} ({asset['type']}) from {conn_name}")
        
        print(f"\n Outputs ({len(outputs)}):")
        for asset in outputs:
            conn_name = asset.get('connection_info', {}).get('name', 'Unknown')
            print(f"   - {asset['name']} ({asset['type']}) from {conn_name}")
        
        auto_confirm = bool(defaults.get('auto_confirm'))
        if not auto_confirm:
            confirm = input("\n✅ Create this lineage? (y/n): ").strip().lower()
            if confirm != 'y':
                print("❌ Lineage creation cancelled")
                return False
        
        success = self.create_lineage_process(inputs, outputs)
        
        if success:
            print("\n🎉 Lineage created successfully!")
            print("🔍 You can now view the lineage in Atlan's UI")
            return True
        else:
            print("\n❌ Failed to create lineage. Check the logs for details.")
            return False

def _read_yaml_config(path: Optional[str]) -> Dict:
    """Read YAML config file if provided, else return empty dict."""
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.error(f"❌ Config file not found: {path}")
        return {}
    except Exception as e:
        logger.error(f"❌ Failed to read config file '{path}': {e}")
        return {}

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Atlan Interactive Lineage Creator")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        required=False,
        help="Path to YAML config file containing Atlan credentials",
    )
    args = parser.parse_args()

    config = _read_yaml_config(args.config)
    creator = InteractiveLineageCreator(config=config)
    success = creator.run()
    
    if success:
        print("\n✅ Lineage creation completed successfully!")
        return 0
    else:
        print("\n❌ Script failed. Check the logs for details.")
        return 1

if __name__ == "__main__":
    exit(main())