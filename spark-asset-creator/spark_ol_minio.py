# spark_customer_analytics_demo.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# CONFIGURATION - Update these values for your environment
# =============================================================================

def _get_atlan_api_key():
    """Read Atlan API key from ATLAN_API_KEY env var or from file ATLAN_API_KEY_FILE (no secrets in code)."""
    key = os.environ.get("ATLAN_API_KEY", "").strip()
    if key:
        return key
    path = os.environ.get("ATLAN_API_KEY_FILE", "").strip()
    if path and os.path.isfile(path):
        with open(path, "r") as f:
            return f.read().strip()
    raise RuntimeError(
        "Atlan API key required. Set ATLAN_API_KEY or ATLAN_API_KEY_FILE (path to token file). "
        "Do not commit tokens to the repo."
    )

# Atlan OpenLineage Configuration
atlan_base_url = "https://home.atlan.com"
atlan_api_key = _get_atlan_api_key()

# CRITICAL NAMESPACE CONFIGURATION:
# Two namespaces are needed for proper lineage in Atlan:
# 
# 1. spark_namespace: The SPARK connection name in Atlan
#    - This is what Atlan's OpenLineage endpoint uses to accept/route events
#    - Must match your Spark connection name for events to be received
#
# 2. s3_namespace: The S3 connection name in Atlan  
#    - This is what Atlan needs to match S3 datasets for lineage
#    - Must match your S3 connection name for lineage to work
#
# WHY TWO NAMESPACES?
# - Atlan's OpenLineage listener validates events against the Spark connection
# - But lineage matching requires datasets to reference the S3 connection
# - We'll use spark_namespace for jobs and try to set s3_namespace for datasets

spark_namespace = "manojs-spark-dev"        # Your Spark connection name in Atlan
s3_namespace = "manojs-s3-analytics"        # Your S3 connection name in Atlan; atlan might have a bug here TBD

# MinIO Configuration (S3-compatible object storage)
s3_endpoint = "http://127.0.0.1:9000"
s3_access_key = "minio"
s3_secret_key = "minio123"

# S3 Paths for input/output
s3_bucket = "demo-bucket-cust"
input_path = f"s3a://{s3_bucket}/raw/customer_analytics_2025_102.csv"
output_path = f"s3a://{s3_bucket}/curated/customer_analytics_agg_2025_102/"

# =============================================================================
# Spark Session with OpenLineage Integration
# =============================================================================

spark = (
    SparkSession.builder
    .appName("CustomerAnalytics-OL-Demo")
    .master("local[*]")
    
    # Spark packages
    .config("spark.jars.packages", "io.openlineage:openlineage-spark_2.12:1.38.0,org.apache.hadoop:hadoop-aws:3.3.4")
    
    # OpenLineage listener
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    
    # OpenLineage transport configuration
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", atlan_base_url)
    .config("spark.openlineage.transport.endpoint", "/events/openlineage/spark/api/v1/lineage")
    .config("spark.openlineage.transport.auth.type", "api_key")
    .config("spark.openlineage.transport.auth.apiKey", atlan_api_key)
    
    # CRITICAL: Set namespace to SPARK connection name so Atlan accepts events
    # This tells Atlan which Spark connection should receive these OpenLineage events
    # Note: Dataset namespaces (S3) are auto-extracted by OpenLineage from S3 URIs
    .config("spark.openlineage.namespace", spark_namespace)
    
    # S3A/Hadoop configuration for MinIO
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    
    .getOrCreate()
)

print("=" * 70)
print("OPENLINEAGE CONFIGURATION")
print("=" * 70)
print(f"Job Namespace (Spark Connection): {spark_namespace}")
print(f"Dataset Namespace (Auto-extracted): s3://{s3_bucket}")
print(f"Desired Dataset Namespace (S3 Connection): {s3_namespace}")
print()
print("⚠️  IMPORTANT:")
print(f"   Events will be sent to Spark connection: {spark_namespace}")
print(f"   Datasets will use auto-extracted namespace: s3://{s3_bucket}")
print(f"   For lineage to work, Atlan Support must map:")
print(f"     s3://{s3_bucket} → {s3_namespace}")
print("=" * 70)

df = spark.read.csv(input_path, header=True)
df_agg = df.groupBy("cust_id").agg(
    F.sum(F.col("ccrev").cast("double")).alias("total_revenue"),
    F.avg(F.col("avg_cc_tran_amt").cast("double")).alias("avg_transaction_amt"),
    F.avg(F.col("avg_cc_tran_cnt").cast("double")).alias("avg_transaction_cnt")
)
df_agg.write.mode("overwrite").parquet(output_path)
spark.stop()