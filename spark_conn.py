"""Standalone Spark connection script with resilient dependency handling.

Fallback order for dependencies:
  1. Remote Maven packages (delta + hadoop-azure + azure libs)
  2. Local JAR directory (env SPARK_LOCAL_JARS_DIR) if remote fails (air-gapped / SSL issues)
  3. Minimal session without extra packages (skips Delta/ABFS features)

Notes on previous failure:
  - Trailing commas turned variable assignments into tuples, breaking config keys.
  - Using delta-core instead of delta-spark is not recommended; prefer io.delta:delta-spark_2.12:<version>.
  - Group id for modern Azure SDK is 'com.azure' not 'com.microsoft.azure'.
  - SSL PKIX failures mean Java truststore lacks the CA chain for those repos; remedy via installing proper JDK and importing corporate root cert if needed.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession  # type: ignore

load_dotenv()

STORAGE_ACCOUNT_NAME = "cadcplitdevlake4h5"
TENANT_ID = os.getenv("AZURE_AD_TENANT_ID")
CLIENT_ID = os.getenv("CAD_AZURE_AD_CLIENT_ID")
CLIENT_SECRET = os.getenv("CAD_AZURE_AD_CLIENT_SECRET")
CONTAINER_NAME = "synapse"

SKIP_SPARK = os.getenv("SKIP_SPARK") == "1"


def find_local_jars():
    jar_dir = os.getenv("SPARK_LOCAL_JARS_DIR")
    if not jar_dir:
        return ""
    p = Path(jar_dir)
    if not p.is_dir():
        print(f"Local JAR dir not found: {p}")
        return ""
    jars = sorted(str(x) for x in p.glob("*.jar"))
    if not jars:
        print(f"No JARs found in {p}")
        return ""
    return ",".join(jars)


def build_spark():
    if SKIP_SPARK:
        print("SKIP_SPARK=1 set; not creating Spark session.")
        return None, {"delta": False, "abfs": False}

    base = (
        SparkSession.builder.appName("ReadAzureDeltaLake")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    remote_pkg_list = [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-azure:3.3.4",
        # Optional newer Azure client libs (not strictly needed for ABFS OAuth but useful if advanced auth used):
        "com.azure:azure-storage-blob:12.25.0",
        "com.azure:azure-identity:1.11.2",
    ]
    remote_pkgs = ",".join(remote_pkg_list)

    # Attempt remote dependency resolution
    try:
        print("Attempting Spark session with remote packages...")
        spark = base.config("spark.jars.packages", remote_pkgs).getOrCreate()
        return spark, {"delta": True, "abfs": True}
    except Exception as e1:
        print(f"[Remote packages failed] {e1}\nTrying local JAR fallback...")
        try:
            local_jars = find_local_jars()
            if local_jars:
                spark = base.config("spark.jars", local_jars).getOrCreate()
                # Heuristic: if delta + hadoop-azure jars included locally, assume features True.
                delta_ok = "delta" in local_jars
                abfs_ok = "hadoop-azure" in local_jars
                return spark, {"delta": delta_ok, "abfs": abfs_ok}
            else:
                raise RuntimeError("No local JARs available for fallback.")
        except Exception as e2:
            print(f"[Local JAR fallback failed] {e2}\nTrying minimal session...")
            try:
                spark = base.getOrCreate()  # minimal (no packages config)
                return spark, {"delta": False, "abfs": False}
            except Exception as e3:
                print(f"[Minimal session failed] {e3}\nAborting Spark initialization.")
                return None, {"delta": False, "abfs": False}


spark, features = build_spark()


def configure_abfs_oauth(spark_session):
    if not spark_session:
        return False
    if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID]):
        print("Missing OAuth environment variables; cannot configure ABFS.")
        return False
    host = f"{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    spark_session.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark_session.conf.set(
        f"fs.azure.account.oauth.provider.type.{host}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark_session.conf.set(f"fs.azure.account.oauth2.client.id.{host}", CLIENT_ID)
    spark_session.conf.set(
        f"fs.azure.account.oauth2.client.secret.{host}", CLIENT_SECRET
    )
    spark_session.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{host}",
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
    )
    return True


abfs_configured = False
if features.get("abfs"):
    abfs_configured = configure_abfs_oauth(spark)
else:
    print("ABFS features not available (missing hadoop-azure jar).")

if spark and features.get("delta") and abfs_configured:
    # Example Delta path (adjust to real path as needed)
    delta_table_path = (
        f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
        "synapse/workspaces/cad-cplit-dev-syn-4h5/warehouse/shipping/geofences_v1r0/"
    )
    try:
        df = spark.read.format("delta").load(delta_table_path)
        df.show(truncate=False)
    except Exception as e:
        print(
            f"Delta read failed: {e}\nCheck that path exists and credentials are valid."
        )
else:
    if spark:
        print(
            "Delta or ABFS not configured; skipping read. See messages above for remediation."
        )
    else:
        print("Spark session unavailable.")

print("Spark initialization summary:")
print(f"  Spark available: {bool(spark)}")
print(f"  Delta enabled: {features.get('delta')}")
print(f"  ABFS enabled: {features.get('abfs')} (configured: {abfs_configured})")
print("Remediation tips if packages failed due to SSL:")
print("  - Install full JDK and set JAVA_HOME")
print("  - Import corporate root CA into JDK truststore if behind intercepting proxy")
print("  - Set HTTPS_PROXY/HTTP_PROXY env vars if needed")
print("  - For offline use: download required JARs and set SPARK_LOCAL_JARS_DIR")
