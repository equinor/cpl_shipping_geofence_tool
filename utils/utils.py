import base64
import io
import json
import math
import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from delta import DeltaTable  # <-- Add this import
from dotenv import load_dotenv
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql import functions as F
from shapely import wkt
from shapely.geometry import mapping

ENABLE_DELETE_BUTTON = True  # Set to True to enable the delete functionality

# Optional Cognite client integration (guarded import to avoid hard failure when library absent)
try:
    from cognite.client import CogniteClient  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    CogniteClient = None


def get_cognite_client():  # pragma: no cover - simple factory
    """Return an initialized CogniteClient if available, else raise a clear error.

    Expects environment variables COGNITE_API_KEY and COGNITE_PROJECT.
    If the cognite sdk isn't installed, raises RuntimeError so calling code can decide to skip.
    """
    if CogniteClient is None:
        raise RuntimeError("cognite-client library not installed.")
    api_key = os.getenv("COGNITE_API_KEY")
    project = os.getenv("COGNITE_PROJECT")
    if not api_key or not project:
        raise RuntimeError(
            "Missing COGNITE_API_KEY or COGNITE_PROJECT environment variables."
        )
    return CogniteClient(api_key=api_key, project=project)


########## Spark related ##############

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


def get_spark_session():
    spark, features = build_spark()
    abfs_configured = False
    if features.get("abfs"):
        abfs_configured = configure_abfs_oauth(spark)
    else:
        print("ABFS features not available (missing hadoop-azure jar).")

    if spark and features.get("delta") and abfs_configured:
        return spark
    else:
        raise RuntimeError(
            "Spark session not properly configured for Delta Lake on ABFS."
        )


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


def parse_contents(contents, filename):
    """
    Parse uploaded geojson file contents and return the geojson dict.
    """
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    try:
        if filename.lower().endswith(".geojson") or filename.lower().endswith(".json"):
            geojson_data = json.load(io.StringIO(decoded.decode("utf-8")))
            return geojson_data
    except Exception as e:
        print(f"Error parsing geojson: {e}")
        return None
    return None


def clean_wkt_string(wkt_string):
    """
    Clean WKT string by removing spaces before '(', after ')', and before/after commas.

    Example: 'POLYGON ((-47.47, 59.21), (-50.99, 52.42))'
    Becomes: 'POLYGON((-47.47,59.21),(-50.99,52.42))'
    """
    if not wkt_string:
        return wkt_string

    # Remove space before '('
    wkt_string = wkt_string.replace(" (", "(")
    # Remove space after ')'
    wkt_string = wkt_string.replace(") ", ")")
    # Remove space before ','
    wkt_string = wkt_string.replace(" ,", ",")
    # Remove space after ','
    wkt_string = wkt_string.replace(", ", ",")

    return wkt_string


def calculate_heading_angle(start_point, end_point):
    """
    Calculate the heading angle (bearing) between two points in degrees.

    Args:
        start_point: [longitude, latitude] of start point
        end_point: [longitude, latitude] of end point

    Returns:
        Heading angle in degrees (0-360, where 0/360 is North)
    """
    lon1, lat1 = math.radians(start_point[0]), math.radians(start_point[1])
    lon2, lat2 = math.radians(end_point[0]), math.radians(end_point[1])

    dlon = lon2 - lon1

    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(
        dlon
    )

    bearing = math.atan2(y, x)
    bearing = math.degrees(bearing)
    bearing = (bearing + 360) % 360  # Normalize to 0-360

    return bearing


def get_all_geofences(client):
    gfs = client.geospatial.list_features(feature_type_external_id="geofence", limit=-1)
    return {x.port_name: x.location["wkt"] for x in gfs}


def wkt_to_geojson(wkt_str):
    polygon = wkt.loads(wkt_str)
    return json.loads(
        json.dumps({"type": "Polygon", "coordinates": [list(polygon.exterior.coords)]})
    )


def populate_geofences():
    client = get_cognite_client()
    gfs_all = get_all_geofences(client)

    features = []
    for name, polygon_wkt in gfs_all.items():
        polygon = wkt.loads(polygon_wkt)
        geojson_feature = {
            "type": "Feature",
            "geometry": mapping(polygon),
            "properties": {"name": name, "tooltip": name},
        }
        features.append(geojson_feature)

    geojson_polygons = {"type": "FeatureCollection", "features": features}
    return geojson_polygons


def get_all_geofence_events(geofence, period):
    client = get_cognite_client()
    all_events = client.events.list(
        type="geofence_arrival", subtype=geofence, limit=-1
    ).to_pandas()
    plot_data = (
        all_events.groupby(all_events["start_time"].dt.to_period(period))
        .size()
        .to_frame(name="count")
        .reset_index()
    )
    fig = go.Figure(go.Bar(x=plot_data["start_time"].astype(str), y=plot_data["count"]))
    fig.update_layout(title={"text": f"Number of events from the geofence {geofence}"})

    return all_events, fig


def get_all_geofence_events_from_cad(conn, geofence, period):
    """Get all geofence events from CAD database and create a plot."""
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT * FROM [sm].[geofence_events_v3r1]
        WHERE GEOFENCE = '{geofence}'
    """
    )
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    all_events = pd.DataFrame.from_records(rows, columns=columns)

    plot_data = (
        all_events.groupby(all_events["ENTRY_TIME"].dt.to_period(period))
        .size()
        .to_frame(name="count")
        .reset_index()
    )
    fig = go.Figure(go.Bar(x=plot_data["ENTRY_TIME"].astype(str), y=plot_data["count"]))
    fig.update_layout(title={"text": f"Number of events from the geofence {geofence}"})

    cursor.close()
    return all_events, fig


def populate_geofences_from_cad(conn):
    """Populate geofences from CAD database and return as GeoJSON."""
    geofences = conn.execute(
        "SELECT [geofence_name], [port_name], [wkt_coordinates], [srid] FROM [sm].[geofences_v1r0]"
    ).fetchall()

    features = []
    for gf in geofences:
        name = gf.port_name
        polygon_wkt = gf.wkt_coordinates
        polygon = wkt.loads(polygon_wkt)
        geojson_feature = {
            "type": "Feature",
            "geometry": mapping(polygon),
            "properties": {
                "name": name,
                "tooltip": name,
                "geofence_name": gf.geofence_name,
            },
        }
        features.append(geojson_feature)

    geojson_polygons = {"type": "FeatureCollection", "features": features}
    return geojson_polygons


def insert_geofences_into_sql(
    conn,
    geofence_name: str,
    port_name: str,
    wkt_coordinates: str,
    srid: int = 4326,
):
    """Insert a geofence record into the CAD SQL database.

    Args:
        conn: An active pyodbc connection.
        geofence_name: Unique identifier for the geofence (logical name).
        port_name: Human-readable port / location name.
        wkt_coordinates: WKT representation of the polygon geometry.
        srid: Spatial reference identifier (defaults to 4326).

    Returns:
        (success: bool, message: str)
    """
    if conn is None:
        return False, "No database connection available."

    if not all([geofence_name, port_name, wkt_coordinates]):
        return (
            False,
            "Missing required fields: geofence_name, port_name, or wkt_coordinates.",
        )

    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO [sm].[geofences_v1r0] ([geofence_name], [port_name], [wkt_coordinates], [srid])
            VALUES (?, ?, ?, ?)
            """
        cursor.execute(
            insert_query,
            (geofence_name.strip(), port_name.strip(), wkt_coordinates.strip(), srid),
        )
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        return True, f"Inserted into [sm].[geofences_v1r0]. Rows affected: {affected}."
    except Exception as e:  # pragma: no cover - defensive
        try:
            cursor.close()  # Ensure cleanup if partially executed
        except Exception:
            pass
        return False, f"Error inserting geofence: {e}"


def insert_geofences_into_spark_table(
    spark,
    geofence_name: str,
    port_name: str,
    wkt_coordinates: str,
    reentry_forbidden_hours: int = 0,
):
    geofences = pd.DataFrame.from_records(
        [(geofence_name, port_name, wkt_coordinates, reentry_forbidden_hours)],
        columns=["external_id", "port_name", "location", "reentry_forbidden_hours"],
    )

    # Create spark DF from the pandas DF
    sdf = spark.createDataFrame(geofences)
    # Convert StringType WKT → MapType {"wkt": "<polygon wkt>"}
    sdf = sdf.withColumn(
        "location",
        F.when(F.col("location").isNull(), F.lit(None)).otherwise(
            F.create_map(F.lit("wkt"), F.col("location"))
        ),
    )
    delta_table_path = (
        f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
        "synapse/workspaces/cad-cplit-dev-syn-4h5/warehouse/shipping/geofences_v1r0/"
    )
    try:
        sdf.write.format("delta").mode("append").save(delta_table_path)
    except Exception as e:
        return False, f"Error inserting geofence into Spark table: {e}"
    return True, "Inserted into Spark table successfully."


def delete_geofence_from_spark_table(
    spark,
    geofence_name: str,
):
    delta_table_path = (
        f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
        "synapse/workspaces/cad-cplit-dev-syn-4h5/warehouse/shipping/geofences_v1r0/"
    )
    try:
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.delete(F.col("external_id") == geofence_name)
    except Exception as e:
        return False, f"Error deleting geofence from Spark table: {e}"
    return True, "Deleted from Spark table successfully."


def get_trajectories(conn, start_time, end_time, vessel_type, geofences):
    """Get vessel trajectories based on geofence events and filters."""
    cursor = conn.cursor()
    sql_stmt = f"""
        WITH passages AS (
            SELECT
                ge."VESSEL_KEY" AS vessel_key,
                ge."GEOFENCE"   AS geofence,
                COALESCE(ge."EXIT_TIME", ge."ENTRY_TIME") AS passage_time
            FROM sm.geofence_events_v3r1 AS ge
            JOIN sm.current_vessel_positions_v1r0 AS cvp
                ON cvp.imo = ge."IMO"
            WHERE CAST(COALESCE(ge."EXIT_TIME", ge."ENTRY_TIME") AS date)
                  BETWEEN '{str(start_time)[0:10]}' AND '{str(end_time)[0:10]}'
              AND cvp.vessel_type IN ({', '.join(f"'{vt}'" for vt in vessel_type)})
              AND ge."GEOFENCE" IN ({', '.join(f"'{gf}'" for gf in geofences)})
        )
        SELECT
            t."VESSEL_KEY" AS vessel_key,
            p.geofence,
            p.passage_time,
            t."LATITUDE" AS latitude,
            t."LONGITUDE" AS longitude,
            t."DT_POS_UTC" AS dt_pos_utc
        FROM sm.trajectories_v1r1 AS t
        JOIN passages AS p
          ON p.vessel_key = t."VESSEL_KEY"
         AND ABS(DATEDIFF(day, t."DT_POS_UTC", p.passage_time)) <= 14;
    """

    # Debug: print SQL statement with line numbers
    for i, line in enumerate(sql_stmt.splitlines(), start=1):
        print(f"{i:02d}: {line}")

    cursor.execute(sql_stmt)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    trajectories = pd.DataFrame.from_records(rows, columns=columns)
    cursor.close()
    return trajectories


def get_trajectory_cargo(conn, start_time, end_time, vessel_type, geofences, join_date):
    """Get vessel trajectories with associated cargo data."""
    if join_date == "unload_date":
        date_join_stmt = "ABS(DATEDIFF(day, c.unload_date, p.passage_time)) < 40"
        date_diff_stmt = "ABS(DATEDIFF(day, c.unload_date, p.passage_time))"
    elif join_date == "load_date":
        date_join_stmt = "ABS(DATEDIFF(day, c.load_date, p.passage_time)) < 40"
        date_diff_stmt = "ABS(DATEDIFF(day, c.load_date, p.passage_time))"
    else:
        # Default to unload_date
        date_join_stmt = "ABS(DATEDIFF(day, c.unload_date, p.passage_time)) < 40"
        date_diff_stmt = "ABS(DATEDIFF(day, c.unload_date, p.passage_time))"

    sql_stmt = f"""
        WITH passages AS (
            SELECT
                ge."VESSEL_KEY" AS vessel_key,
                ge."GEOFENCE"   AS geofence,
                ge."EXIT_HEADING" AS exit_heading,
                CASE
                    WHEN ge."EXIT_HEADING" >= 337.5 OR ge."EXIT_HEADING" < 22.5 THEN 'N'
                    WHEN ge."EXIT_HEADING" >= 22.5 AND ge."EXIT_HEADING" < 67.5 THEN 'NE'
                    WHEN ge."EXIT_HEADING" >= 67.5 AND ge."EXIT_HEADING" < 112.5 THEN 'E'
                    WHEN ge."EXIT_HEADING" >= 112.5 AND ge."EXIT_HEADING" < 157.5 THEN 'SE'
                    WHEN ge."EXIT_HEADING" >= 157.5 AND ge."EXIT_HEADING" < 202.5 THEN 'S'
                    WHEN ge."EXIT_HEADING" >= 202.5 AND ge."EXIT_HEADING" < 247.5 THEN 'SW'
                    WHEN ge."EXIT_HEADING" >= 247.5 AND ge."EXIT_HEADING" < 292.5 THEN 'W'
                    WHEN ge."EXIT_HEADING" >= 292.5 AND ge."EXIT_HEADING" < 337.5 THEN 'NW'
                    ELSE NULL
                END AS exit_direction,
                COALESCE(ge."EXIT_TIME", ge."ENTRY_TIME") AS passage_time
            FROM sm.geofence_events_v3r0 AS ge
            JOIN sm.current_vessel_positions_v1r0 AS cvp
                ON cvp.imo = ge."IMO"
            WHERE CAST(COALESCE(ge."EXIT_TIME", ge."ENTRY_TIME") AS date)
                    BETWEEN '{str(start_time)[0:10]}' AND '{str(end_time)[0:10]}'
                AND cvp.vessel_type IN ({', '.join(f"'{vt}'" for vt in vessel_type)})
                AND ge."GEOFENCE" IN ({', '.join(f"'{gf}'" for gf in geofences)})
        ),
        cargo_with_min_diff AS (
            SELECT
                p.*,
                c.*,
                {date_diff_stmt} AS date_diff,
                ROW_NUMBER() OVER (
                    PARTITION BY p.vessel_key, p.passage_time
                    ORDER BY {date_diff_stmt}
                ) AS rn
            FROM passages p
            LEFT JOIN sm.cargoflow_v1r2 c
                ON p.vessel_key = c.vessel_imo
                AND {date_join_stmt}
        )
        SELECT
            vessel_key,
            geofence,
            exit_heading,
            exit_direction,
            date_diff,
            unload_region,
            unload_country,
            unload_port,
            unload_terminal,
            load_date,
            unload_date,
            load_region,
            load_country,
            load_port,
            load_terminal,
            passage_time,
            [group],
            group_product,
            category,
            grade,
            quantity,
            vessel_type,
            vessel_class,
            cargo_status,
            via_geofence
        FROM cargo_with_min_diff
        WHERE rn = 1 OR rn IS NULL  -- Keep the best match or records with no match
        ORDER BY vessel_key, passage_time;
    """

    cursor = conn.cursor()
    cursor.execute(sql_stmt)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    result = pd.DataFrame.from_records(rows, columns=columns)
    cursor.close()
    return result
