import base64
import io
import json
import math

import pandas as pd
import plotly.graph_objects as go
from shapely import wkt
from shapely.geometry import mapping

from utils.cdf import get_cognite_client

# Configuration settings
ENABLE_DELETE_BUTTON = False  # Set to True to enable the delete functionality


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
