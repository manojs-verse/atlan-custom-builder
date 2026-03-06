## Atlan Custom Builder

This repository hosts custom assets, lineage, and solution work for Atlan.

### Documentation
- **[OpenLineage Spark-to-S3 Lineage Guide](OPENLINEAGE_LINEAGE_GUIDE.md)** - Complete guide for setting up OpenLineage with Spark and S3, including troubleshooting the two-namespace problem
- **README.md (this file)** - Project overview and general usage

### Sub-Projects
This repository includes multiple tools for working with Atlan:
1. **Interactive Lineage Creator** - Create lineage between assets (table or column-level), with optional 1:1 column mapping and batch configs from Excel
2. **Custom Connection Creator** - Create custom connections in Atlan (KNOWN or CUSTOM connector types)
3. **Relational Assets Creator** - Create Database / Schema / Table assets under an existing connection
4. **Object Store Assets Creator** - Create S3-compatible bucket and object assets (S3, MinIO, ECS, Wasabi, etc.)
5. **BI Assets Creator** - Create Tableau projects, workbooks, and dashboards under an existing connection
6. **App Assets Creator** - Create Application and ApplicationField assets under an existing App connection
7. **Spark OpenLineage Integration** - Send Spark job lineage to Atlan via OpenLineage (MinIO demo)

---

## Interactive Lineage Creator

Create lineage between assets in Atlan via an interactive CLI or fully automated from config. Search for assets across connections and stitch inputs → outputs into `Process` entities using the official `pyatlan` SDK.

### Features
- **Interactive search**: pick a connection, then find assets by qualified name, targeted search (database/schema), or simple name search.
- **Supported asset types**: `Table`, `View`, `Column`, `S3Object`, `S3Bucket`, `Application`, `ApplicationField`, and Power BI types (`PowerBITable`, `PowerBIDataset`, `PowerBIReport`, `PowerBIDashboard`, `PowerBIColumn`, `PowerBIMeasure`, `PowerBIWorkspace`, `PowerBIDataflow`, `PowerBIPage`, `PowerBITile`, `PowerBIDatasource`).
- **Column-level lineage**: use column qualified names in `inputs` and `outputs` to create column-level lineage in Atlan.
- **1:1 column lineage mode**: set `column_lineage_1to1: true` in config so that `input[i]` is linked only to `output[i]` (col1→col1, col2→col2, …). Without it, one process is created with all inputs and all outputs (many-to-many).
- **Process creation**: builds a descriptive name, accepts a custom Process ID (or per-column IDs in 1:1 mode), optional SQL and Source URL, then saves lineage to Atlan.
- **Batch from Excel**: use Qualified Names exported from Atlan (e.g. from xlsx “Qualified Name” column) in config to map source columns to destination columns 1:1; example configs: `config-fedex.yaml`, `config-fedex2.yaml`, `config-fedex3.yaml`.
- **Logging**: writes to `logs/lineage_creation.log` and streams to the console.

### Requirements
- Python 3.9+
- Packages: `pyatlan`, `python-dotenv`, `pyyaml`

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
# Create a .env file with BASE_URL and API_KEY (see below)

# Create your config from the template
cp config.example.yaml config.yaml

# Run via helper script
./run.sh -c config.yaml

# Or run directly
python create_lineage_interactive.py --config config.yaml
```

If you prefer, you can install dependencies via a requirements file of your own. The root does not ship a consolidated requirements file; module-specific files are under sub-projects (e.g., `spark-asset-creator/requirements.txt`).

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
When you run the script interactively (or when config defaults are missing), you'll be guided through:
1. **Select INPUT assets (sources)**
   - Choose a connection
   - Choose an asset type (Table, View, Column, S3Object, S3Bucket, Application, ApplicationField, or Power BI types)
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
- Initializes an `AtlanClient` using `BASE_URL` and `API_KEY` (from config or env).
- Resolves `defaults.inputs` and `defaults.outputs` by qualified name (or falls back to interactive selection).
- **1:1 mode** (`column_lineage_1to1: true` and equal-length lists): creates one `Process` per (input[i], output[i]) with `process_id` = `{base_process_id}_{column_name}`, so Atlan shows col1→col1, col2→col2, etc. Re-runs update the same processes.
- **Default mode**: creates a single `Process` with all input refs and all output refs (many-to-many lineage).
- Uses `Process.creator(...)` with asset references by GUID and saves via `client.asset.save(process)`.

### Optional Non-Interactive Defaults
You can provide defaults in the YAML to skip prompts and run fully automated:

```yaml
defaults:
  inputs:
    - "<qualified_name_of_input_asset>"
    # For column-level lineage, list column qualified names (one per source column).
  outputs:
    - "<qualified_name_of_output_asset>"
    # Same order as inputs for 1:1 mapping when column_lineage_1to1 is true.
  process_id: "etl_job_123"      # optional; in 1:1 mode becomes base: "<process_id>_<column_name>"
  sql: "SELECT 1"                # optional
  source_url: "https://example"  # optional
  auto_confirm: true             # if true, use defaults without prompting
  process_connection_qualified_name: "default/snowflake/12345"  # optional override for Process
  # 1:1 column lineage: one Process per (input[i], output[i]) so col1→col1, col2→col2, …
  column_lineage_1to1: true      # optional; when true and len(inputs)==len(outputs), creates N processes
```

Notes:
- If `inputs` or `outputs` are omitted or cannot be resolved, the tool falls back to interactive selection for that role.
- If `auto_confirm` is false/omitted, defaults act as prefilled values and you'll still be prompted.
- **1:1 mode**: when `column_lineage_1to1: true` and inputs/outputs have the same length, the tool creates one Process per pair (input[i] → output[i]). Each process gets a stable ID `{process_id}_{column_name}` so re-runs update the same processes.
- **Without 1:1 mode**: a single Process is created with all inputs and all outputs (Atlan shows many-to-many lineage between them).

### Batch configs from Excel (source → destination column lineage)
You can build a config from Atlan exports (xlsx) that include a “Qualified Name” column:

1. Export source and destination table columns from Atlan (e.g. `mysql.xlsx`, `bigquery.xlsx`).
2. Align columns 1:1 by name (e.g. by the “Title” column in the export).
3. Put source column Qualified Names in `defaults.inputs` and destination column Qualified Names in `defaults.outputs` in the same order.
4. Set `column_lineage_1to1: true` and a stable `process_id` (e.g. `p365`).
5. Run: `./run.sh -c config-fedex.yaml -m lineage`.

Example configs in this repo (replace credentials as needed):
- **config-fedex.yaml** – `rfid_scan_events_raw` (mssql → bigquery), 10 columns, process_id `p365`
- **config-fedex2.yaml** – `facility_reference_raw` (mssql → bigquery), 14 columns, process_id `p366`
- **config-fedex3.yaml** – `shipment_manifest_raw` (mssql → bigquery), 16 columns, process_id `p367`

Each uses a distinct `process_id` so re-runs update the same processes and do not create duplicates.

### Troubleshooting
- **No connections found**: Verify the API token permissions and that the account can view the relevant connections.
- **Auth errors (401/403)**: Double-check `BASE_URL` and `API_KEY` values and scope.
- **Slow or empty asset results**: Use targeted search with database/schema and exact case where possible.
- **Duplicates**: Use a stable `process_id` (and in 1:1 mode, each column gets `process_id_<column_name>`). Re-runs then update the same processes.
- **Logging**: Inspect `logs/lineage_creation.log` for details.

### Development Notes
- Main script: `create_lineage_interactive.py`
- Logging: configured for both file and stdout
- Extensibility: add more asset types by updating the `supported_asset_types` map in the script.

### Helper Script
`run.sh` wraps venv creation and dependency installation, then runs the chosen module with a provided config:

```bash
./run.sh -c config.yaml -m lineage
```

Flags:
- `-c, --config`: path to YAML config (required)
- `-m, --module`: which module to run: `lineage` (default), `connection`, `relational`, `object_store`, `bi`, or `app`
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
- Only S3-compatible providers are supported. Set `provider: "s3"`. The script validates the connection is S3-compatible (accepts ECS/MinIO/etc.).
- Set `object_store_assets.connection_qualified_name` or `connection_name` to target the right connection. You can look it up via the lineage tool or Atlan UI.
- If your connection’s connector type shows as Unknown, set `assume_s3_compatible: true` to bypass type checks.
- Strict SDKs may require ARNs. Provide:
  - `buckets[].aws_arn` (e.g., `arn:aws:s3:::my-bucket`)
  - `buckets[].objects[].aws_arn` (e.g., `arn:aws:s3:::my-bucket/path/to/file`)
- Enable detailed logs during troubleshooting with `object_store_assets.debug: true`.

### Sub-project: BI Assets Creator

Create Tableau projects / workbooks / dashboards under an existing connection.

```bash
./run.sh -c config.yaml -m bi

# Or run directly
python create_bi_assets.py --config config.yaml
```

Config: see section `bi_assets` in `config.example.yaml`.

### Sub-project: App Assets Creator

Create Application and ApplicationField assets under an existing App connection. Create the App connection first via `-m connection` with `connector.known_type: "APP"` (or CUSTOM if APP is not in your SDK).

```bash
./run.sh -c config.yaml -m app

# Or run directly
python create_app_assets.py --config config.yaml
```

Config: see section `app_assets` in `config.example.yaml`. You can link existing assets to application fields via `owned_assets` (list of qualified names).

### Sub-project: Spark OpenLineage Integration (MinIO demo)

Send Spark job lineage events to Atlan using OpenLineage, with a local MinIO S3-compatible store for inputs/outputs.

Prerequisites:
- Docker (for MinIO)
- Java 11+ and Spark 3.5 locally (spark-submit available)
- An Atlan API key with OpenLineage access

Quickstart:
```bash
cd spark-asset-creator

# 1) Start MinIO and seed demo data (bucket + CSV)
./001-run-minio-docker.sh

# 2) Provide your Atlan API key for OpenLineage
echo "<YOUR_ATLAN_API_KEY>" > atlan_token.txt

# 3) (Optional) Create venv and install dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 4) Run the Spark job with OpenLineage configured
./run-spark-job.sh
```

Notes:
- Edit `spark-asset-creator/spark_ol_minio.py` to set `spark_namespace` (your Spark connection name in Atlan) and adjust `s3_bucket`, `input_path`, and `output_path` as needed.
- Due to the OpenLineage S3 dataset extractor, dataset namespaces default to `s3://<bucket>`. For lineage linking in Atlan, follow the guidance in `OPENLINEAGE_LINEAGE_GUIDE.md` (Two-Namespace Problem).
- More details and troubleshooting: see `OPENLINEAGE_LINEAGE_GUIDE.md`.

## Modules & user flow

1) Configure credentials in `config.yaml` under `atlan`.
2) Choose a module to run via `./run.sh -c config.yaml -m <module>`:
   - **lineage** (default): create lineage between assets (interactive or from config; supports 1:1 column lineage and batch configs from Excel).
   - **connection**: create a connection (KNOWN or CUSTOM) and optionally seed relational assets.
   - **relational**: create databases / schemas / tables under a connection.
   - **object_store**: create S3-compatible bucket and object assets.
   - **bi**: create Tableau projects / workbooks / dashboards.
   - **app**: create Application and ApplicationField assets under an App connection.
   - **spark-asset-creator**: local Spark + OpenLineage demo with MinIO (see section above; run from `spark-asset-creator/`).

## Configuration reference

- **Global credentials**: `atlan.base_url`, `atlan.api_key` (required by all modules)
- **Lineage** (`defaults`; optional; only used by `lineage`):
  - `inputs`, `outputs`: lists of asset qualified names (use column QNs for column-level lineage)
  - `process_id`, `sql`, `source_url`, `auto_confirm`, `process_connection_qualified_name`
  - `column_lineage_1to1` (bool): when true and len(inputs)==len(outputs), creates one Process per pair (col1→col1, col2→col2, …)
- **Connection**: `custom_connection` (optional; used by `connection`)
  - `connector.use`: `KNOWN` or `CUSTOM`
  - `connector.known_type`: e.g., `SNOWFLAKE`, `S3`, `BIGQUERY`, `APP` (fallback to CUSTOM if not in SDK)
  - `connector.custom`: `name`, `value`, `category`
- **Relational assets**: `relational_assets` (optional; used by `relational`)
- **Object store assets**: `object_store_assets` (optional; used by `object_store`)
  - `provider: "s3"`, `connection_name` or `connection_qualified_name`
  - `assume_s3_compatible` (bool), `debug` (bool)
  - `buckets[].name`, optional `buckets[].aws_arn`, and `objects[].key`, optional `objects[].aws_arn`
- **BI assets**: `bi_assets` (optional; used by `bi`)
- **App assets**: `app_assets` (optional; used by `app`); `applications[].fields[].owned_assets` for linking existing assets

### Example configs
- **Lineage (batch column 1:1)**: `config-fedex.yaml`, `config-fedex2.yaml`, `config-fedex3.yaml` — source→destination column lineage from Excel Qualified Names; each uses a distinct `process_id` (p365, p366, p367).
- **Other modules**: see `backup-configs/` for examples (e.g., `backup-configs/config2.yaml`, `backup-configs/config6.yaml`).
- Example commands:
  ```bash
  ./run.sh -c config-fedex.yaml -m lineage
  ./run.sh -c backup-configs/config2.yaml -m object_store
  ```

## Iceberg (recommended approach)

- Prefer using the query engine’s connector that fronts your Iceberg catalog (e.g., `TRINO`, `DREMIO`, `ATHENA`, `SNOWFLAKE`). Create that connection via `-m connection`, then model Iceberg tables using `-m relational`.
- If your SDK exposes `ICEBERG` in `AtlanConnectorType`, you can set `connector.use: KNOWN` and `known_type: "ICEBERG"`. Otherwise, a `CUSTOM` connector in category `DATABASE` also works.

## Troubleshooting

- Object store creation fails with required kwargs: add explicit ARNs for bucket/object and enable `debug: true` to see attempted parameters.
- Provider mismatch or Unknown connector: set `assume_s3_compatible: true` or create a KNOWN `S3` connection.
- KNOWN connector not found (e.g., `DREMIO`): we now fall back to `CUSTOM` automatically and proceed; logs list available enums.
- Can’t resolve connection by name: use `connection_qualified_name` instead.
- Logs: see `logs/*.log` for each module.

### License
Apache License 2.0. See `LICENSE`.

### Attribution
Based on Atlan developer snippets for lineage management: `https://developer.atlan.com/snippets/common-examples/lineage/manage/#directly`.


