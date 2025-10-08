## Atlan Custom Builder

This repository hosts custom assets, lineage, and solution work for Atlan. The first sub-project is the Interactive Lineage Creator; more sub-projects will be added over time.

### Sub-project: Interactive Lineage Creator

Create lineage between assets in Atlan via an interactive CLI. Search for assets (Tables, Views, Columns) across connections and stitch inputs → outputs into a `Process` using the official `pyatlan` SDK.

### Features
- **Interactive search**: pick a connection, then find assets by qualified name, targeted search (database/schema), or simple name search.
- **Supported asset types**: `Table`, `View`, `Column`.
- **Process creation**: builds a descriptive name, accepts a custom Process ID, optional SQL and Source URL, then saves lineage to Atlan.
- **Logging**: writes to `logs/lineage_creation.log` and streams to the console.

### Requirements
- Python 3.9+
- Packages: `pyatlan`, `python-dotenv`

### Quickstart
```bash
# Clone
git clone <this-repo-url>
cd atlan-custom-builder

# (Recommended) Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate

# Install dependencies (or use run.sh which installs them for you)
pip install pyatlan python-dotenv pyyaml

# Configure environment
cp .env.example .env  # or create .env as shown below

# Create your config from the template
cp config.example.yaml config.yaml

# Run via helper script
./run.sh -c config.yaml

# Or run directly
python create_lineage_interactive.py --config config.yaml
```

If you prefer, you can install dependencies via a requirements file of your own. This repository does not ship one by default.

### Configuration
You can provide credentials via YAML config or environment variables. A `.env` file is also supported via `python-dotenv`.

```bash
# config.yaml (recommended)
atlan:
  base_url: "https://your-tenant.atlan.com"   # e.g., https://example.atlan.com
  api_key: "atlan_api_token_here"             # Personal Access Token from Atlan

# or .env
BASE_URL="https://your-tenant.atlan.com"
API_KEY="atlan_api_token_here"
```

Notes:
- Ensure the API token has sufficient permissions to search assets and create lineage processes.
- The script loads `.env` automatically at startup.

### Usage Flow
When you run the script, you'll be guided through these steps:
1. **Select INPUT assets (sources)**
   - Choose a connection
   - Choose an asset type (`Table`, `View`, or `Column`)
   - Choose a search method:
     - Direct qualified name (fastest)
     - Targeted search (database/schema/name)
     - Simple name search (may be slower)
   - Pick one or more assets
2. **Select OUTPUT assets (targets)**
   - Repeat the selection flow for outputs
3. **Review summary and confirm**
4. **Provide details for the `Process`**
   - Process ID (unique identifier; auto-generated if left blank)
   - Optional SQL
   - Optional Source URL
5. **Create lineage**
   - The tool constructs a `Process` and saves it via `pyatlan`

### Example Run
```bash
python create_lineage_interactive.py
```
You will be prompted to select connections and assets and then confirm creation. Logs are written to `logs/lineage_creation.log`.

### How It Works
- Initializes an `AtlanClient` using `BASE_URL` and `API_KEY`.
- Searches connections and assets using `FluentSearch` with `CompoundQuery`.
- Creates a `Process` via `Process.creator(...)` with references to selected input/output assets by GUID.
- Saves the resulting lineage to Atlan with `client.asset.save(process)`.

### Optional Non-Interactive Defaults
You can provide simple defaults in the YAML to skip prompts:

```yaml
defaults:
  inputs:
    - "<qualified_name_of_input_asset>"
  outputs:
    - "<qualified_name_of_output_asset>"
  process_id: "etl_job_123"      # optional
  sql: "SELECT 1"                # optional
  source_url: "https://example"  # optional
  auto_confirm: true              # if true, use defaults without prompting
  process_connection_qualified_name: "default/snowflake/12345"  # optional override for Process
```

Notes:
- If `inputs` or `outputs` are omitted or cannot be resolved, the tool falls back to interactive selection for that role.
- If `auto_confirm` is false/omitted, defaults act as prefilled values and you'll still be prompted.

### Troubleshooting
- **No connections found**: Verify the API token permissions and that the account can view the relevant connections.
- **Auth errors (401/403)**: Double-check `BASE_URL` and `API_KEY` values and scope.
- **Slow or empty asset results**: Use targeted search with database/schema and exact case where possible.
- **Duplicates**: Re-using the same Process ID may cause conflicts or multiple similar processes. Prefer stable, unique IDs per pipeline/job run.
- **Logging**: Inspect `logs/lineage_creation.log` for details.

### Development Notes
- Main script: `create_lineage_interactive.py`
- Logging: configured for both file and stdout
- Extensibility: add more asset types by updating the `supported_asset_types` map in the script.

### Helper Script
`run.sh` wraps venv creation and dependency installation, then runs the script with a provided config:

```bash
./run.sh -c config.yaml -m lineage
```

Flags:
- `-c, --config`: path to YAML config (required)
- `-m, --module`: which module to run: `lineage` (default) or `connection`
- `--recreate-venv`: force recreation of the local `.venv`

### Sub-project: Custom Connection & Assets Creator

Create a new custom connection in Atlan (optionally with a Database, Schema, and Tables) using `pyatlan`.

#### Usage
```bash
# Create a connection using settings under custom_connection in config.yaml
./run.sh -c config.yaml -m connection

# Or run directly
python create_custom_connection.py --config config.yaml
```

#### Configuration
Add a `custom_connection` section to your `config.yaml` (see `config.example.yaml` for a fuller example):

```yaml
custom_connection:
  name: "my-custom-conn"
  connector:
    use: "CUSTOM"           # KNOWN or CUSTOM
    known_type: "API"       # used when KNOWN (e.g., SNOWFLAKE, BIGQUERY, API)
    custom:
      name: "MY_CUSTOM"
      value: "my-custom"
      category: "API"
  admin_roles:
    - "$admin"
  create_assets:
    enabled: true
    database: "EXAMPLE_DB"
    schema: "PUBLIC"
    tables:
      - name: "SAMPLE_TABLE"
        description: "Created by automation"
```

Notes:
- If `connector.use: KNOWN`, the `known_type` should match a value from `AtlanConnectorType` (e.g., `SNOWFLAKE`, `BIGQUERY`, `API`).
- If `connector.use: CUSTOM`, the script attempts to register the custom connector via `AtlanConnectorType.CREATE_CUSTOM`. If unsupported in your SDK version, it falls back to `API`.
- `admin_roles` are resolved by name (e.g., `$admin`) to GUIDs.

### Sub-project: Relational Assets Creator

Create Database / Schema / Tables under an existing connection.

```bash
./run.sh -c config.yaml -m relational

# Or run directly
python create_relational_assets.py --config config.yaml
```

Config: see section `relational_assets` in `config.example.yaml`.

### Sub-project: Object Store Assets Creator

Create object store bucket and object assets under an existing S3-compatible connection (S3, ECS, MinIO, Wasabi, etc.).

```bash
./run.sh -c config.yaml -m object_store

# Or run directly
python create_object_store_assets.py --config config.yaml
```

Config: see section `object_store_assets` in `config.example.yaml`.

Notes:
- Only S3-compatible providers are supported. Set provider to `s3`. The script validates the connection is S3-compatible (accepts ECS/MinIO/etc.).
- Set `object_store_assets.connection_qualified_name` to the correct connection’s qualified name. You can look it up via the lineage tool or Atlan UI.

### Sub-project: BI Assets Creator

Create Tableau projects / workbooks / dashboards under an existing connection.

```bash
./run.sh -c config.yaml -m bi

# Or run directly
python create_bi_assets.py --config config.yaml
```

Config: see section `bi_assets` in `config.example.yaml`.


### License
Apache License 2.0. See `LICENSE`.

### Attribution
Based on Atlan developer snippets for lineage management: `https://developer.atlan.com/snippets/common-examples/lineage/manage/#directly`.


