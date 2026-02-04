# OpenLineage Spark-to-S3 Lineage Guide

This guide explains how to set up OpenLineage with Spark and Atlan for S3 lineage, including troubleshooting common namespace issues.

---

## Table of Contents
1. [Quick Start](#quick-start)
2. [The Two-Namespace Problem](#the-two-namespace-problem)
3. [Configuration](#configuration)
4. [Known Issues & Solutions](#known-issues--solutions)
5. [Atlan Support Request](#atlan-support-request)
6. [Verification Steps](#verification-steps)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites
1. **Atlan Connections**:
   - Spark connection (e.g., `manojs-spark-dev`)
   - S3 connection (e.g., `manojs-s3-analytics`)

2. **S3 Assets Created**:
   ```bash
   ./run.sh -c config2.yaml -m object_store
   ```

3. **Spark Job with OpenLineage**:
   ```bash
   cd spark-asset-creator
   ./run-spark-job.sh
   ```

---

## The Two-Namespace Problem

### Overview

When using OpenLineage with Spark and S3 in Atlan, **two separate namespaces** are required:

| Namespace Type | Purpose | Configuration | Value |
|---------------|---------|---------------|-------|
| **Job Namespace** | Tell Atlan which Spark connection receives events | `spark.openlineage.namespace` | Spark connection name |
| **Dataset Namespace** | Tell Atlan which S3 connection datasets belong to | Auto-extracted from S3 URIs | S3 URI (e.g., `s3://bucket-name`) |

### The Problem

```
OpenLineage Event Structure:
{
  "job": {
    "namespace": "manojs-spark-dev"  ← From spark.openlineage.namespace
  },
  "inputs": [
    {
      "name": "raw/customer_analytics_2025_102.csv",
      "namespace": "s3://demo-bucket-cust"  ← Auto-extracted by OpenLineage
    }
  ],
  "outputs": [
    {
      "name": "curated/customer_analytics_agg_2025_102",
      "namespace": "s3://demo-bucket-cust"  ← Auto-extracted by OpenLineage
    }
  ]
}

Atlan Expectations:
├── Job namespace = "manojs-spark-dev" ✅ (Spark connection for event routing)
└── Dataset namespace = "manojs-s3-analytics" ❌ (S3 connection for lineage matching)
                         BUT receives: "s3://demo-bucket-cust"
```

### Why This Happens

1. **`spark.openlineage.namespace` only affects job namespace**, not dataset namespace
2. **OpenLineage's S3A dataset extractor automatically uses the S3 URI** (`s3://bucket-name`) as the namespace
3. **This cannot be overridden** in OpenLineage Spark Agent version 1.38.0 through configuration
4. **Atlan expects the dataset namespace to match the S3 connection name** for lineage linking

### Impact

- ✅ **Events are accepted** by Atlan (job namespace matches Spark connection)
- ❌ **Lineage is NOT linked** (dataset namespace doesn't match S3 connection)

---

## Configuration

### 1. Spark Configuration (`spark-asset-creator/spark_ol_minio.py`)

```python
# CRITICAL: Use SPARK connection name for job namespace
spark_namespace = "manojs-spark-dev"        # Your Spark connection name
s3_namespace = "manojs-s3-analytics"        # Your S3 connection name (for reference)

spark = (
    SparkSession.builder
    .appName("CustomerAnalytics-OL-Demo")
    
    # OpenLineage transport
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "https://home.atlan.com")
    .config("spark.openlineage.transport.endpoint", "/events/openlineage/spark/api/v1/lineage")
    .config("spark.openlineage.transport.auth.type", "api_key")
    .config("spark.openlineage.transport.auth.apiKey", atlan_api_key)
    
    # Use SPARK connection name for job namespace
    .config("spark.openlineage.namespace", spark_namespace)
    
    .getOrCreate()
)
```

### 2. S3 Asset Configuration (`config2.yaml`)

```yaml
object_store_assets:
  enabled: true
  connection_qualified_name: "default/s3/1760111720"
  connection_name: "manojs-s3-analytics"  # Must match S3 connection in Atlan
  provider: "S3"
  buckets:
    - name: "demo-bucket-cust"
      region: "us-west-1"
      aws_arn: "arn:aws:s3:::demo-bucket-cust"
      objects:
        - key: "raw/customer_analytics_2025_102.csv"
          name: "customer_analytics_2025_102.csv"
          aws_arn: "arn:aws:s3:::demo-bucket-cust/raw/customer_analytics_2025_102.csv"
        - key: "curated/customer_analytics_agg_2025_102"
          name: "customer_analytics_agg_2025_102"
          aws_arn: "arn:aws:s3:::demo-bucket-cust/curated/customer_analytics_agg_2025_102"
  assume_s3_compatible: true
```

---

## Known Issues & Solutions

### Issue 1: ARN Field Showing Incorrect Format ✅ FIXED

**Problem**: ARN field in Atlan UI showed `demo-bucket-cust/raw/file.csv` instead of `arn:aws:s3:::demo-bucket-cust/raw/file.csv`

**Solution**: Fixed in `create_object_store_assets.py` to use proper AWS ARN format:
```python
obj_arn = (obj.get('aws_arn') or f"arn:aws:s3:::{bucket_name}/{key}").strip()
```

### Issue 2: Events Not Received When Using S3 Connection Name ✅ FIXED

**Problem**: Setting `spark.openlineage.namespace = "manojs-s3-analytics"` caused Atlan to reject events

**Solution**: Use Spark connection name for job namespace:
```python
spark_namespace = "manojs-spark-dev"  # Spark connection for event acceptance
.config("spark.openlineage.namespace", spark_namespace)
```

### Issue 3: Lineage Not Being Linked ⚠️ REQUIRES ATLAN SUPPORT

**Problem**: Events received but lineage not linked due to dataset namespace mismatch

**Solution**: Contact Atlan Support for backend namespace mapping (see next section)

### Issue 4: Uniqueness Constraint Violation

**Problem**: Error when recreating assets:
```
SchemaViolationException: Adding this property for key [__u_awsArn] violates a uniqueness constraint
```

**Solution**: Assets already exist. Either:
1. Skip creation if assets exist
2. Delete old assets before recreating
3. Use update/merge instead of create

---

## Atlan Support Request

Since OpenLineage's S3 dataset namespace cannot be overridden through configuration, **Atlan Support must configure a backend namespace mapping**.

### Support Request Template

```
Subject: OpenLineage Namespace Mapping Request for S3 Lineage

Hello Atlan Support,

We need a namespace mapping configured for OpenLineage S3 lineage to work correctly.

CURRENT CONFIGURATION:
- Spark Connection Name: manojs-spark-dev
- S3 Connection Name: manojs-s3-analytics
- S3 Bucket: demo-bucket-cust
- S3 Connection QN: default/s3/1760111720

THE ISSUE:
OpenLineage automatically sets the dataset namespace to "s3://demo-bucket-cust" 
(the S3 URI), but our S3 connection in Atlan is named "manojs-s3-analytics".

OpenLineage events show:
- job.namespace: "manojs-spark-dev" ✅ (correctly matches Spark connection)
- inputs[].namespace: "s3://demo-bucket-cust" ❌ (doesn't match S3 connection)
- outputs[].namespace: "s3://demo-bucket-cust" ❌ (doesn't match S3 connection)

Atlan S3 objects have qualified names like:
- default/s3/1760111720/demo-bucket-cust/raw/customer_analytics_2025_102.csv
- default/s3/1760111720/demo-bucket-cust/curated/customer_analytics_agg_2025_102

SOLUTION NEEDED:
Can you configure a namespace mapping in Atlan's OpenLineage processor to map:
  "s3://demo-bucket-cust" → "manojs-s3-analytics"

This will allow Atlan to correctly match S3 datasets in OpenLineage events to 
our S3 connection and establish lineage between Spark jobs and S3 objects.

ATTACHED:
- Sample OpenLineage events (from spark-asset-creator/log files from atlan/)
- Spark configuration (spark_ol_minio.py)

Thank you for your assistance!
```

---

## Verification Steps

### Before Atlan Support Mapping

1. **Create S3 Assets**:
   ```bash
   ./run.sh -c config2.yaml -m object_store
   ```
   
   **Expected**: Assets created successfully with proper ARN format

2. **Run Spark Job**:
   ```bash
   cd spark-asset-creator
   ./run-spark-job.sh
   ```
   
   **Expected**: 
   - ✅ Events are received by Atlan
   - ✅ Events appear in Spark connection (`manojs-spark-dev`)
   - ❌ Lineage is NOT linked yet

3. **Check Atlan UI**:
   - Navigate to Spark connection → Verify events appear
   - Check S3 objects → Verify no lineage shown yet

### After Atlan Support Mapping

1. **Run Spark Job Again**:
   ```bash
   cd spark-asset-creator
   ./run-spark-job.sh
   ```

2. **Check Lineage in Atlan UI**:
   - Navigate to Spark job → Check lineage graph
   - Should show connections:
     - Spark Job → S3 Input (`raw/customer_analytics_2025_102.csv`)
     - Spark Job → S3 Output (`curated/customer_analytics_agg_2025_102`)
   - Verify column-level lineage is working

3. **Check S3 Objects**:
   - Navigate to S3 objects
   - Should show upstream/downstream lineage to Spark jobs

---

## Troubleshooting

### Events Not Appearing in Atlan

**Check**:
1. API key is valid and has permissions
2. `spark.openlineage.namespace` matches Spark connection name
3. Network connectivity to Atlan
4. Check Spark driver logs for OpenLineage errors

**Debug**:
```bash
# Check Spark output for OpenLineage messages
cd spark-asset-creator
./run-spark-job.sh 2>&1 | grep -i "openlineage"
```

### Lineage Not Linked (After Namespace Mapping)

**Check**:
1. Atlan Support confirmed namespace mapping is active
2. S3 object qualified names are correct
3. OpenLineage events have correct dataset names (should match S3 object keys)

**Debug**:
- Check OpenLineage event structure in Atlan UI
- Verify dataset `name` and `namespace` fields
- Confirm S3 object qualified names match expected pattern

### ARN Field Issues

**Check**:
1. `config2.yaml` has proper `aws_arn` format: `arn:aws:s3:::bucket/key`
2. `create_object_store_assets.py` is using the latest version

**Expected ARN Format**:
- Bucket: `arn:aws:s3:::bucket-name`
- Object: `arn:aws:s3:::bucket-name/path/to/object`

### Uniqueness Constraint Violations

**Issue**: Assets with same ARN already exist

**Solutions**:
1. **Delete existing assets** in Atlan UI before recreating
2. **Use different bucket/object names** for testing
3. **Update existing assets** instead of creating new ones

---

## Alternative Workarounds (NOT RECOMMENDED)

If Atlan Support cannot configure namespace mapping, these are fallback options:

### Option A: Event Rewrite Proxy
- Create a proxy service that intercepts OpenLineage events
- Rewrite dataset namespaces from `s3://bucket` to S3 connection name
- Forward modified events to Atlan
- **Cons**: Additional infrastructure, maintenance overhead

### Option B: Rename S3 Connection
- Rename S3 connection in Atlan to match S3 URI (e.g., `s3://demo-bucket-cust`)
- **Cons**: Loses semantic naming, organizational impact

### Option C: Custom OpenLineage Build
- Build custom OpenLineage Spark agent with modified namespace extraction
- **Cons**: Significant development effort, ongoing maintenance

---

## Why Atlan Support Solution is Best

✅ **No code changes** - Works with standard OpenLineage  
✅ **No infrastructure overhead** - Handled by Atlan backend  
✅ **Semantic naming preserved** - Keep meaningful connection names  
✅ **Standard practice** - Common requirement in lineage platforms  
✅ **Future-proof** - Works with OpenLineage version upgrades  

---

## References

- [OpenLineage Specification](https://openlineage.io/spec/)
- [OpenLineage Spark Integration](https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark)
- [Atlan OpenLineage Documentation](https://developer.atlan.com/integrations/openlineage/)
- [PyAtlan SDK](https://github.com/atlanhq/atlan-python)

---

## Summary Checklist

- [ ] S3 assets created with correct ARN format
- [ ] Spark configured with `spark_namespace` = Spark connection name
- [ ] Spark job runs and events appear in Atlan Spark connection
- [ ] Contacted Atlan Support for namespace mapping
- [ ] After mapping: Lineage appears between Spark jobs and S3 objects
- [ ] Column-level lineage is working

