import json
import os
import struct

import dash
import dash_leaflet as dl
import msal
import pandas as pd
import plotly.graph_objs as go
import pyodbc
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dotenv import load_dotenv
from shapely.geometry import shape

from utils.utils import (
    ENABLE_DELETE_BUTTON,
    calculate_heading_angle,
    clean_wkt_string,
    delete_geofence_from_spark_table,
    get_all_geofence_events_from_cad,
    get_spark_session,
    get_trajectories,
    get_trajectory_cargo,
    insert_geofences_into_spark_table,
    insert_geofences_into_sql,
    parse_contents,
    populate_geofences_from_cad,
)

load_dotenv()

# Azure AD / SQL configuration
CAD_AZURE_AD_CLIENT_ID = os.getenv("CAD_AZURE_AD_CLIENT_ID")
AZURE_AD_TENANT_ID = os.getenv("AZURE_AD_TENANT_ID")
CAD_AZURE_AD_CLIENT_SECRET = os.getenv("CAD_AZURE_AD_CLIENT_SECRET")
AUTHORITY_URL = f"https://login.microsoftonline.com/{AZURE_AD_TENANT_ID}"
SQL_SERVER = "cad-cplit-dev-sql-4h5.database.windows.net"
DATABASE = "citizen_db"

# Acquire token (client credentials)
msal_app = msal.ConfidentialClientApplication(
    CAD_AZURE_AD_CLIENT_ID,
    authority=AUTHORITY_URL,
    client_credential=CAD_AZURE_AD_CLIENT_SECRET,
)
result = msal_app.acquire_token_for_client(
    scopes=["https://database.windows.net//.default"]
)

conn = None
CONNECTION_ERROR = None
if result and "access_token" in result:
    access_token = result["access_token"]
    PORT = "1433"
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{SQL_SERVER},{PORT};"
        f"Database={DATABASE};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )
    token_bytes = access_token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    try:
        conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    except Exception as e:
        CONNECTION_ERROR = f"DB connection failed: {e}"
else:
    CONNECTION_ERROR = (
        f"Token error: {result.get('error')} {result.get('error_description')}"
    )

spark = get_spark_session()

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div(
    [
        html.H2("Explore geofences (env = dev)"),
        dcc.Tabs(
            id="tabs-example",
            value="tab-1",
            children=[
                dcc.Tab(label="Deploy geofences", value="tab-1"),
                dcc.Tab(label="View geofences", value="tab-2"),
            ],
        ),
        html.Div(id="tabs-content-example"),
    ]
)

# Provide a validation_layout so Dash knows about components that appear only after switching tabs.
# This prevents "nonexistent object" errors for dynamic children like the GeoJSON with id="polygons".
app.validation_layout = html.Div(
    [
        html.H2(),  # dummy
        dcc.Tabs(
            id="tabs-example",
            value="tab-1",
            children=[
                dcc.Tab(label="Deploy geofences", value="tab-1"),
                dcc.Tab(label="View geofences", value="tab-2"),
            ],
        ),
        html.Div(id="tabs-content-example"),
        # All IDs referenced in callbacks below:
        dl.GeoJSON(id="polygons", data={"type": "FeatureCollection", "features": []}),
        dcc.RadioItems(id="period"),
        dcc.RadioItems(id="join-date-radio"),
        html.Button(id="clear-trajectory-btn"),
        html.Button(id="delete-btn"),
        html.Button(id="export-trajectory-btn"),
        html.Button(id="export-table-btn"),
        html.Button(id="generate-chart-btn"),
        html.Button(id="deploy-idle-geofence-btn"),
        dcc.Input(id="duration"),
        dcc.Dropdown(id="vessel-dropdown"),
        dcc.Dropdown(id="chart-type-dropdown"),
        dcc.Dropdown(id="x-axis-dropdown"),
        dcc.Dropdown(id="y-axis-dropdown"),
        dcc.Dropdown(id="aggregation-dropdown"),
        dcc.Dropdown(id="color-dropdown"),
        dcc.Graph(id="dynamic-chart"),
        dl.Map(id="map_2"),
        dcc.Store(id="selected-geofence"),
        dcc.Store(id="trajectory-data"),
        dcc.Store(id="table-data"),
        dcc.Download(id="download-trajectories"),
        dcc.Download(id="download-table"),
        html.Div(id="trajectory-output"),
        html.Pre(id="deploy-idle-response"),
        html.Pre(id="heading-display"),
        dcc.ConfirmDialog(id="confirm-delete"),
        dash.dash_table.DataTable(id="geofence-table"),
    ]
)


@app.callback(
    Output("tabs-content-example", "children"), Input("tabs-example", "value")
)
def render_content(tab):
    global CONNECTION_ERROR
    if tab == "tab-1":
        return html.Div(
            [
                html.H3("Deploy geofences"),
                html.Div(
                    [
                        dcc.Input(id="port_name", type="text", placeholder="Port Name"),
                        dcc.Input(
                            id="geofence_name", type="text", placeholder="Geofence Name"
                        ),
                        dcc.Upload(
                            id="upload-data",
                            children=html.Button("Upload a geojson"),
                            multiple=False,
                        ),
                        dl.Map(
                            id="map_1",
                            center=[56, 10],
                            zoom=3,
                            children=[
                                dl.TileLayer(),
                                dl.FeatureGroup(
                                    [
                                        dl.EditControl(
                                            id="draw-control",
                                            position="topleft",
                                            draw=dict(
                                                polyline=True,
                                                polygon=True,
                                                circle=False,
                                                rectangle=False,
                                                marker=False,
                                                circlemarker=False,
                                            ),
                                        )
                                    ]
                                ),
                            ],
                            style={
                                "width": "90%",
                                "height": "70vh",
                                "margin": "auto",
                                "display": "block",
                            },
                        ),
                        html.Pre(
                            "Polygon coordinates will be shown here",
                            id="polygon-coords",
                            style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                        ),
                        # Buttons to write the polygons to different tables
                        html.Div(
                            [
                                html.Button(
                                    "Deploy Geofence",
                                    id="deploy-geofence-btn",
                                    n_clicks=0,
                                    style={"marginRight": "10px"},
                                ),
                                html.Button(
                                    "Deploy Idle Geofence",
                                    id="deploy-idle-geofence-btn",
                                    n_clicks=0,
                                ),
                            ],
                            style={"marginBottom": "10px"},
                        ),
                        html.Pre(
                            "Heading angle will be shown here when a line is drawn",
                            id="heading-display",
                            style={
                                "whiteSpace": "pre-wrap",
                                "wordBreak": "break-all",
                                "backgroundColor": "#f0f0f0",
                                "padding": "10px",
                            },
                        ),
                        html.Pre(
                            "Response from deployment will be shown here",
                            id="deploy-response",
                            style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                        ),
                        html.Pre(
                            "Response from idle geofence deployment will be shown here",
                            id="deploy-idle-response",
                            style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                        ),
                    ]
                ),
            ]
        )
    # Tab 2: view geofences
    polygons_data = {"type": "FeatureCollection", "features": []}
    if conn:
        try:
            polygons_data = populate_geofences_from_cad(conn)
        except Exception as e:
            CONNECTION_ERROR = f"Failed loading geofences: {e}"
    return html.Div(
        [
            html.Div(
                [
                    html.H3("View geofences", style={"margin": 0}),
                    html.Button(
                        "Delete Geofence",
                        id="delete-btn",
                        n_clicks=0,
                        disabled=not ENABLE_DELETE_BUTTON,
                        title="Enable Delete from utils",
                        style={
                            "backgroundColor": "#ff4d4d"
                            if ENABLE_DELETE_BUTTON
                            else "#cccccc",
                            "color": "white",
                            "border": "none",
                            "padding": "8px 14px",
                            "borderRadius": "4px",
                            "cursor": "pointer"
                            if ENABLE_DELETE_BUTTON
                            else "not-allowed",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "6px",
                },
            ),
            html.Div(
                [
                    dl.Map(
                        id="map_2",
                        center=[25, 0],
                        zoom=3,
                        children=[
                            dl.TileLayer(),
                            dl.GeoJSON(data=polygons_data, id="polygons"),
                        ],
                        style={
                            "width": "90%",
                            "height": "70vh",
                            "margin": "auto",
                            "display": "block",
                        },
                    )
                ]
            ),
            html.Div(
                children=[
                    (
                        html.Div(
                            CONNECTION_ERROR,
                            style={"color": "red", "marginBottom": "8px"},
                        )
                        if CONNECTION_ERROR
                        else html.Div()
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    dcc.Dropdown(
                                        id="vessel-dropdown",
                                        options=[
                                            {
                                                "label": "LPG Carriers",
                                                "value": "LPG Carriers",
                                            },
                                            {
                                                "label": "Oil Tankers",
                                                "value": "Oil Tankers",
                                            },
                                            {
                                                "label": "LNG Carriers",
                                                "value": "LNG Carriers",
                                            },
                                        ],
                                        multi=True,
                                        placeholder="Select vessels",
                                        style={"width": "200px", "marginRight": "8px"},
                                    ),
                                    html.Span("Last", style={"marginRight": "4px"}),
                                    dcc.Input(
                                        id="duration",
                                        type="number",
                                        placeholder="7",
                                        value=7,
                                        min=1,
                                        max=365,
                                        style={"width": "40px", "marginRight": "4px"},
                                    ),
                                    html.Span("days", style={"marginRight": "8px"}),
                                    html.Button(
                                        "Clear Trajectories",
                                        id="clear-trajectory-btn",
                                        n_clicks=0,
                                        style={"marginRight": "8px"},
                                    ),
                                    html.Button(
                                        "Export Trajectories",
                                        id="export-trajectory-btn",
                                        n_clicks=0,
                                    ),
                                ],
                                style={
                                    "display": "flex",
                                    "alignItems": "center",
                                    "marginBottom": "6px",
                                },
                            ),
                        ],
                        style={"display": "flex", "align-items": "center"},
                    ),
                    html.Div(id="trajectory-output"),
                    html.H3(
                        "Frequency of plot:",
                        style={"margin-right": "10px", "line-height": "36px"},
                    ),
                    dcc.RadioItems(
                        options=[
                            {"label": "Daily", "value": "D"},
                            {"label": "Weekly", "value": "W"},
                            {"label": "Monthy", "value": "M"},
                        ],
                        value="D",
                        inline=True,
                        id="period",
                    ),
                    html.Pre(
                        id="polygon-coords-display", style={"whiteSpace": "pre-wrap"}
                    ),
                    dcc.Graph(id="geofence_events"),
                    html.H3("Vessels in the Trajectories"),
                    html.Div(
                        [
                            html.H4(
                                "Join Date Type:",
                                style={"margin-right": "10px", "line-height": "36px"},
                            ),
                            dcc.RadioItems(
                                options=[
                                    {"label": "Load Date", "value": "load_date"},
                                    {"label": "Unload Date", "value": "unload_date"},
                                ],
                                value="unload_date",
                                inline=True,
                                id="join-date-radio",
                                style={"marginBottom": "10px"},
                            ),
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "marginBottom": "8px",
                        },
                    ),
                    # export table by export button
                    html.Button("Export Table", id="export-table-btn", n_clicks=0),
                    dash.dash_table.DataTable(
                        id="geofence-table",
                        columns=[
                            {"name": "Vessel Key", "id": "vessel_key", "type": "text"},
                            {"name": "Geofence", "id": "geofence", "type": "text"},
                            {
                                "name": "Exit Heading",
                                "id": "exit_heading",
                                "type": "text",
                            },
                            {
                                "name": "Exit Direction",
                                "id": "exit_direction",
                                "type": "text",
                            },
                            {"name": "Date Diff", "id": "date_diff", "type": "numeric"},
                            {
                                "name": "Unload Region",
                                "id": "unload_region",
                                "type": "text",
                            },
                            {
                                "name": "Unload Country",
                                "id": "unload_country",
                                "type": "text",
                            },
                            {
                                "name": "Unload Port",
                                "id": "unload_port",
                                "type": "text",
                            },
                            {
                                "name": "Unload Terminal",
                                "id": "unload_terminal",
                                "type": "text",
                            },
                            {
                                "name": "Load Region",
                                "id": "load_region",
                                "type": "text",
                            },
                            {
                                "name": "Load Country",
                                "id": "load_country",
                                "type": "text",
                            },
                            {"name": "Load Port", "id": "load_port", "type": "text"},
                            {
                                "name": "Load Terminal",
                                "id": "load_terminal",
                                "type": "text",
                            },
                            {
                                "name": "Passage Time",
                                "id": "passage_time",
                                "type": "text",
                            },
                            {"name": "Group", "id": "group", "type": "text"},
                            {
                                "name": "Group Product",
                                "id": "group_product",
                                "type": "text",
                            },
                            {"name": "Category", "id": "category", "type": "text"},
                            {"name": "Grade", "id": "grade", "type": "text"},
                            {"name": "Quantity", "id": "quantity", "type": "numeric"},
                            {"name": "Load Date", "id": "load_date", "type": "text"},
                            {
                                "name": "Unload Date",
                                "id": "unload_date",
                                "type": "text",
                            },
                            {
                                "name": "Vessel Type",
                                "id": "vessel_type",
                                "type": "text",
                            },
                            {
                                "name": "Vessel Class",
                                "id": "vessel_class",
                                "type": "text",
                            },
                            {
                                "name": "Cargo Status",
                                "id": "cargo_status",
                                "type": "text",
                            },
                            {
                                "name": "Via Geofence",
                                "id": "via_geofence",
                                "type": "text",
                            },
                        ],
                        data=[],
                        filter_action="native",
                        sort_action="native",
                        sort_mode="multi",
                        page_size=10,
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left"},
                        style_header={"fontWeight": "bold"},
                    ),
                    # Dynamic Plotting Section
                    html.Hr(),
                    html.H3("Dynamic Chart Builder", style={"marginTop": "20px"}),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label(
                                        "Chart Type:",
                                        style={
                                            "fontWeight": "bold",
                                            "marginRight": "10px",
                                        },
                                    ),
                                    dcc.Dropdown(
                                        id="chart-type-dropdown",
                                        options=[
                                            {"label": "Bar Chart", "value": "bar"},
                                            {"label": "Line Chart", "value": "line"},
                                            {
                                                "label": "Scatter Plot",
                                                "value": "scatter",
                                            },
                                            {"label": "Pie Chart", "value": "pie"},
                                            {"label": "Box Plot", "value": "box"},
                                            {
                                                "label": "Histogram",
                                                "value": "histogram",
                                            },
                                        ],
                                        value="bar",
                                        style={"width": "200px"},
                                    ),
                                ],
                                style={
                                    "display": "inline-block",
                                    "marginRight": "20px",
                                },
                            ),
                            html.Div(
                                [
                                    html.Label(
                                        "X-Axis:",
                                        style={
                                            "fontWeight": "bold",
                                            "marginRight": "10px",
                                        },
                                    ),
                                    dcc.Dropdown(
                                        id="x-axis-dropdown",
                                        options=[],  # Will be populated dynamically
                                        value=None,
                                        style={"width": "200px"},
                                    ),
                                ],
                                style={
                                    "display": "inline-block",
                                    "marginRight": "20px",
                                },
                            ),
                            html.Div(
                                [
                                    html.Label(
                                        "Y-Axis Column:",
                                        style={
                                            "fontWeight": "bold",
                                            "marginRight": "10px",
                                        },
                                    ),
                                    dcc.Dropdown(
                                        id="y-axis-dropdown",
                                        options=[],  # Will be populated dynamically
                                        value=None,
                                        style={"width": "180px"},
                                    ),
                                ],
                                style={
                                    "display": "inline-block",
                                    "marginRight": "20px",
                                },
                            ),
                            html.Div(
                                [
                                    html.Label(
                                        "Aggregation:",
                                        style={
                                            "fontWeight": "bold",
                                            "marginRight": "10px",
                                        },
                                    ),
                                    dcc.Dropdown(
                                        id="aggregation-dropdown",
                                        options=[
                                            {"label": "Count", "value": "count"},
                                            {"label": "Sum", "value": "sum"},
                                            {"label": "Average", "value": "mean"},
                                            {"label": "Min", "value": "min"},
                                            {"label": "Max", "value": "max"},
                                        ],
                                        value="count",
                                        style={"width": "150px"},
                                    ),
                                ],
                                style={
                                    "display": "inline-block",
                                    "marginRight": "20px",
                                },
                            ),
                            html.Div(
                                [
                                    html.Label(
                                        "Color By:",
                                        style={
                                            "fontWeight": "bold",
                                            "marginRight": "10px",
                                        },
                                    ),
                                    dcc.Dropdown(
                                        id="color-dropdown",
                                        options=[],  # Will be populated dynamically
                                        value=None,
                                        style={"width": "200px"},
                                    ),
                                ],
                                style={
                                    "display": "inline-block",
                                    "marginRight": "20px",
                                },
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        "Generate Chart",
                                        id="generate-chart-btn",
                                        n_clicks=0,
                                        style={
                                            "backgroundColor": "#007bff",
                                            "color": "white",
                                            "border": "none",
                                            "padding": "8px 16px",
                                            "borderRadius": "4px",
                                            "cursor": "pointer",
                                            "marginTop": "23px",
                                        },
                                    )
                                ],
                                style={"display": "inline-block"},
                            ),
                        ],
                        style={"marginBottom": "20px"},
                    ),
                    # Chart Display Area
                    dcc.Graph(id="dynamic-chart", style={"height": "500px"}),
                    dcc.Interval(id="refresh", interval=15 * 1000, n_intervals=0),
                    dcc.Store(id="selected-geofence"),
                    dcc.Store(id="trajectory-data"),
                    dcc.Download(id="download-trajectories"),
                    dcc.Download(id="download-table"),
                    dcc.ConfirmDialog(
                        id="confirm-delete",
                        message="Delete the selected geofence? This action cannot be undone.",
                    ),
                ]
            ),
        ]
    )


# Call-back for geojson upload
@app.callback(
    Output("map_1", "children"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
    State("map_1", "children"),
    prevent_initial_call=True,
)
def upload_geojson(contents, filename, current_children):
    """Append uploaded GeoJSON as a new layer without removing draw controls.

    We preserve existing TileLayer + FeatureGroup(EditControl) and just add/replace an 'uploaded-polygons' layer.
    """
    if contents is None:
        return dash.no_update
    geojson_data = parse_contents(contents, filename)
    if geojson_data is None:
        return dash.no_update
    if current_children is None:
        current_children = []
    # Remove any previous uploaded layer to avoid duplicates
    filtered = [
        c
        for c in current_children
        if not (
            isinstance(c, dict) and c.get("props", {}).get("id") == "uploaded-polygons"
        )
        and getattr(c, "id", None) != "uploaded-polygons"
    ]
    uploaded_layer = dl.GeoJSON(
        data=geojson_data,
        id="uploaded-polygons",
        options={
            "style": {"color": "red", "weight": 1, "opacity": 0.5, "fillOpacity": 0.2}
        },
    )
    return filtered + [uploaded_layer]


@app.callback(
    [Output("polygon-coords", "children"), Output("heading-display", "children")],
    Input("draw-control", "geojson"),
    Input("port_name", "value"),
    Input("geofence_name", "value"),
)
def polygon_data(geojson, port_name, geofence_name):
    if not geojson or not geojson.get("features"):
        return (
            "No polygon data",
            "Heading angle will be shown here when a line is drawn",
        )

    feature = geojson["features"][0]
    geometry = feature["geometry"]
    geometry_type = geometry.get("type", "")

    # Handle different geometry types
    if geometry_type == "LineString":
        # Calculate heading for polylines
        coordinates = geometry["coordinates"]
        if len(coordinates) >= 2:
            start_point = coordinates[0]
            end_point = coordinates[-1]
            heading = calculate_heading_angle(start_point, end_point)

            # Create line info
            line_info = {
                "geometry_type": "LineString",
                "start_point": start_point,
                "end_point": end_point,
                "coordinates": coordinates,
            }

            heading_text = f"Polyline Heading: {heading:.1f}° (from North)\n"
            heading_text += (
                f"Start Point: [{start_point[0]:.6f}, {start_point[1]:.6f}]\n"
            )
            heading_text += f"End Point: [{end_point[0]:.6f}, {end_point[1]:.6f}]\n"
            heading_text += f"Total Points: {len(coordinates)}"

            return json.dumps(line_info), heading_text
        else:
            return "Invalid line data", "Need at least 2 points to calculate heading"

    elif geometry_type == "Polygon":
        # Handle polygons as before
        geom = shape(geometry).wkt
        clean_geom = clean_wkt_string(geom)

        out = json.dumps(
            {
                "geofence_name": geofence_name or "",
                "port_name": port_name or "",
                "wkt_coordinates": clean_geom,
            }
        )

        return out, "Polygon drawn - no heading calculated for polygons"

    else:
        return (
            f"Unsupported geometry type: {geometry_type}",
            "Heading calculation not available for this geometry type",
        )


def _child_id(child):
    if hasattr(child, "id"):
        return getattr(child, "id")
    if isinstance(child, dict):
        return child.get("props", {}).get("id")
    return None


# Show confirmation dialog when delete button clicked and a geofence is selected
@app.callback(
    Output("confirm-delete", "displayed"),
    Input("delete-btn", "n_clicks"),
    State("selected-geofence", "data"),
    prevent_initial_call=True,
)
def show_delete_dialog(n_clicks, geofence_name):
    if not n_clicks:
        return False
    # Only show if delete functionality is enabled and a geofence is selected
    if not ENABLE_DELETE_BUTTON or not geofence_name:
        return False
    return True


# Perform deletion upon user confirmation
@app.callback(
    Output("trajectory-output", "children", allow_duplicate=True),
    Output("map_2", "children", allow_duplicate=True),
    Output("selected-geofence", "data", allow_duplicate=True),
    Input("confirm-delete", "submit_n_clicks"),
    State("selected-geofence", "data"),
    State("map_2", "children"),
    prevent_initial_call=True,
)
def delete_geofence(submit_n_clicks, geofence_name, current_children):
    if not submit_n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    if not ENABLE_DELETE_BUTTON:
        return "Delete functionality is disabled.", dash.no_update, dash.no_update
    if not geofence_name:
        return "No geofence selected to delete.", dash.no_update, dash.no_update
    global CONNECTION_ERROR, conn
    if CONNECTION_ERROR or conn is None:
        return (
            f"Cannot delete due to connection error: {CONNECTION_ERROR}",
            dash.no_update,
            dash.no_update,
        )
    try:
        cursor = conn.cursor()
        # Delete by geofence_name
        cursor.execute(
            "DELETE FROM [sm].[geofences_v1r0] WHERE geofence_name = ?",
            (geofence_name,),
        )
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        # Also delete from spark geofences table
        spark_delete = delete_geofence_from_spark_table(spark, geofence_name)
    except Exception as e:
        return (
            f"Error deleting geofence '{geofence_name}': {e}",
            dash.no_update,
            dash.no_update,
        )
    # Refresh polygons layer after deletion
    try:
        polygons_data = populate_geofences_from_cad(conn)
    except Exception as e:
        polygons_data = {"type": "FeatureCollection", "features": []}
    # Rebuild map children preserving TileLayer but replacing polygons & removing trajectory layer
    if current_children is None:
        current_children = []
    base_children = []
    for c in current_children:
        cid = _child_id(c)
        # Keep only tile layers; drop previous polygons and trajectory layers
        if isinstance(c, dict):
            # Dash components may be dict after transport; check type in props
            comp_type = c.get("type") or c.get("props", {}).get("id")
        else:
            comp_type = type(c).__name__
        if cid == "trajectory-layer":
            continue
        if cid == "polygons":
            continue
        base_children.append(c)
    base_children.append(dl.GeoJSON(data=polygons_data, id="polygons"))
    status = f"Geofence '{geofence_name}' deleted. Rows affected: {affected}."
    return status, base_children, None


@app.callback(
    Output("deploy-response", "children"),
    Input("deploy-geofence-btn", "n_clicks"),
    State("polygon-coords", "children"),
    prevent_initial_call=True,
)
def deploy_geofence(n_clicks, payload):
    if n_clicks is None or n_clicks == 0:
        return None
    global CONNECTION_ERROR
    if CONNECTION_ERROR:
        return f"Cannot deploy geofence due to connection error: {CONNECTION_ERROR}"
    try:
        payload_dict = json.loads(payload)
        # Writing the geofence to the CAD geofences table
        geofence_name = payload_dict.get("geofence_name")
        port_name = payload_dict.get("port_name")
        wkt_coordinates = payload_dict.get("wkt_coordinates")
        sql_write, message = insert_geofences_into_sql(
            conn, geofence_name, port_name, wkt_coordinates, srid=4326
        )
        # Writing the geofence to the spark geofences table
        if sql_write:
            spark_write = insert_geofences_into_spark_table(
                spark, geofence_name, port_name, wkt_coordinates, 0
            )
            return f"Geofence deployed successfully: {message}\nSpark Table Write: {spark_write}"
        else:
            return f"Geofence deployment failed: {message}"
    except Exception as e:
        return f"Error deploying geofence: {e}"


@app.callback(
    Output("deploy-idle-response", "children"),
    Input("deploy-idle-geofence-btn", "n_clicks"),
    State("polygon-coords", "children"),
    prevent_initial_call=True,
)
def deploy_idle_geofence(n_clicks, payload):
    if n_clicks is None or n_clicks == 0:
        return None
    global CONNECTION_ERROR
    if CONNECTION_ERROR:
        return (
            f"Cannot deploy idle geofence due to connection error: {CONNECTION_ERROR}"
        )
    try:
        payload_dict = json.loads(payload)
        # Extract geofence_name, port_name, and wkt_coordinates from payload
        geofence_name = payload_dict.get("geofence_name")
        port_name = payload_dict.get("port_name")
        wkt_coordinates = payload_dict.get("wkt_coordinates")

        srid = 5326

        if not all([geofence_name, port_name, wkt_coordinates]):
            return (
                "Missing required fields: geofence_name, port_name, or wkt_coordinates."
            )

        cursor = conn.cursor()
        insert_query = """
            INSERT INTO [sm].[geofences_idle_v1r0] ([geofence_name], [port_name], [wkt_coordinates], [srid])
            VALUES (?, ?, ?, ?)
        """
        cursor.execute(insert_query, (geofence_name, port_name, wkt_coordinates, srid))
        conn.commit()
        cursor.close()
        # Get response from the cursor (rowcount for insert)
        response = f"Inserted into [sm].[geofences_idle_v1r0] table. Rows affected: {cursor.rowcount}"
        return f"Idle geofence deployed successfully: {response}"
    except Exception as e:
        return f"Error deploying idle geofence: {e}"


@app.callback(
    Output("polygon-coords-display", "children"),
    Output("geofence_events", "figure"),
    Output("selected-geofence", "data"),
    Output("map_2", "children"),
    Output("trajectory-output", "children"),
    Output("trajectory-data", "data"),
    Input("polygons", "clickData"),
    Input("period", "value"),
    Input("clear-trajectory-btn", "n_clicks"),
    Input("duration", "value"),
    Input("vessel-dropdown", "value"),
    State("map_2", "children"),
    State("selected-geofence", "data"),
    prevent_initial_call=True,
)
def display_polygon_coords(
    click_feature,
    period,
    clear_clicks,
    duration,
    vessel_types,
    current_children,
    stored_geofence,
):
    global CONNECTION_ERROR
    if current_children is None:
        current_children = []
    ctx = dash.callback_context
    triggered = ctx.triggered[0]["prop_id"] if ctx.triggered else None

    def remove_trajectory(children):
        return [c for c in children if _child_id(c) != "trajectory-layer"]

    # Clear button triggered: only remove trajectory layer; DO NOT re-fetch trajectories or events
    if triggered and triggered.startswith("clear-trajectory-btn"):
        new_children = remove_trajectory(current_children)
        return (
            dash.no_update
            if stored_geofence
            else "Click a polygon to see its coordinates.",
            dash.no_update,
            stored_geofence,
            new_children,
            "Trajectories cleared." if stored_geofence else "No geofence selected.",
            None,
        )

    # No selection yet
    if click_feature is None:
        return (
            "Click a polygon to see its coordinates.",
            go.Figure(),
            None,
            current_children,
            "No geofence selected.",
            None,
        )

    geometry = click_feature.get("geometry", {})
    properties = click_feature.get("properties", {})
    geofence_name = (
        properties.get("geofence_name") or properties.get("name") or "Unnamed"
    )
    coords = geometry.get("coordinates")

    # Events figure (reload on period change or polygon click)
    try:
        _, fig = get_all_geofence_events_from_cad(conn, geofence_name, period)
    except Exception as e:
        fig = go.Figure()
        fig.add_annotation(text=f"Error loading events: {e}", showarrow=False)

    content = html.Div([html.P(f"Polygon: {geofence_name}, Coordinates: {coords}")])

    # Period change only -> keep trajectories; only update figure
    if triggered and triggered.startswith("period"):
        return (
            content,
            fig,
            geofence_name,
            current_children,
            "Period changed; trajectories kept.",
            dash.no_update,
        )

    # Polygon click -> fetch trajectories
    # Fallback vessel types if dropdown empty
    try:
        traj_df = get_trajectories(
            conn,
            pd.Timestamp.now() - pd.Timedelta(days=duration if duration else 7),
            pd.Timestamp.now(),
            vessel_types,
            [geofence_name],
        )
    except Exception as e:
        return (
            content,
            fig,
            geofence_name,
            current_children,
            f"Error fetching trajectories: {e}",
            None,
        )

    if traj_df.empty:
        new_children = remove_trajectory(current_children)
        return (
            content,
            fig,
            geofence_name,
            new_children,
            f"No trajectory points found for {geofence_name}.",
            None,
        )

    geojson_lines = []
    for vessel_key, group in traj_df.groupby("vessel_key"):
        coords_line = group[["longitude", "latitude"]].values.tolist()
        dates = group["dt_pos_utc"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
        geojson_lines.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords_line},
                "properties": {"name": vessel_key, "dates": dates},
            }
        )
    base_children = remove_trajectory(current_children)

    # Create a GeoJSON FeatureCollection directly for trajectory lines
    feature_collection = {"type": "FeatureCollection", "features": geojson_lines}
    color = "red"
    trajectory_layer = dl.GeoJSON(
        id="trajectory-layer",
        data=feature_collection,
        options={
            "style": {
                "stroke": True,
                "color": color,
                "weight": 2,
                "opacity": 0.8,
            }
        },
        zoomToBounds=False,
        zoomToBoundsOnClick=False,
    )
    new_children = base_children + [trajectory_layer]
    # Enrich stored records with geofence name for later export filename context
    stored_records = traj_df.assign(geofence_name=geofence_name).to_dict("records")
    return (
        content,
        fig,
        geofence_name,
        new_children,
        f"Loaded {len(geojson_lines)} trajectory lines for {geofence_name}.",
        stored_records,
    )


@app.callback(
    Output("download-trajectories", "data"),
    Input("export-trajectory-btn", "n_clicks"),
    State("vessel-dropdown", "value"),
    State("trajectory-data", "data"),
    prevent_initial_call=True,
)
def export_trajectories(n_clicks, vessel_types, trajectory_records):
    if not n_clicks:
        return None
    if not trajectory_records:
        return None
    df = pd.DataFrame(trajectory_records)
    features = []
    for vessel_key, group in df.groupby("vessel_key"):
        coords = group[["longitude", "latitude"]].values.tolist()
        dates = group["dt_pos_utc"].astype(str).tolist()
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {"name": vessel_key, "dates": dates},
            }
        )
    geojson = {"type": "FeatureCollection", "features": features}
    # Use vessel types and geofence name as name, joined by underscore
    vessel_str = "_".join(vessel_types) if vessel_types else "vessel"
    geofence_str = (
        trajectory_records[0]["geofence_name"]
        if trajectory_records and "geofence_name" in trajectory_records[0]
        else "geofence"
    )
    filename = f"{vessel_str}_{geofence_str}_trajectories.geojson"
    return {"content": json.dumps(geojson, indent=2), "filename": filename}


@app.callback(
    Output("geofence-table", "data"),
    [
        Input("selected-geofence", "data"),
        Input("duration", "value"),
        Input("vessel-dropdown", "value"),
        Input("join-date-radio", "value"),
    ],
    prevent_initial_call=True,
)
def populate_cargo_table(selected_geofence, duration, vessel_types, join_date):
    """Populate the table with cargo data when a geofence is selected."""
    if not selected_geofence or not vessel_types:
        return []

    global CONNECTION_ERROR, conn
    if CONNECTION_ERROR or conn is None:
        return []

    try:
        # Calculate time range
        start_time = pd.Timestamp.now() - pd.Timedelta(days=duration if duration else 7)
        end_time = pd.Timestamp.now()

        # Get cargo trajectory data
        cargo_df = get_trajectory_cargo(
            conn, start_time, end_time, vessel_types, [selected_geofence], join_date
        )

        if cargo_df.empty:
            return []

        # Convert to records for the table
        # Handle datetime columns by converting to string
        cargo_df_copy = cargo_df.copy()
        date_columns = ["load_date", "unload_date", "passage_time"]
        for col in date_columns:
            if col in cargo_df_copy.columns:
                cargo_df_copy[col] = cargo_df_copy[col].astype(str)

        return cargo_df_copy.to_dict("records")

    except Exception as e:
        print(f"Error populating cargo table: {e}")
        return []


@app.callback(
    Output("download-table", "data"),
    Input("export-table-btn", "n_clicks"),
    State("geofence-table", "data"),
    prevent_initial_call=True,
)
def export_table(n_clicks, table_data):
    if not n_clicks:
        return None
    if not table_data:
        return None

    df = pd.DataFrame(table_data)

    # Convert to CSV format
    csv_string = df.to_csv(index=False)

    # Generate filename with timestamp
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    filename = f"geofence_cargo_data_{timestamp}.csv"

    return {"content": csv_string, "filename": filename}


# Dynamic Chart Callbacks
@app.callback(
    [
        Output("x-axis-dropdown", "options"),
        Output("y-axis-dropdown", "options"),
        Output("color-dropdown", "options"),
    ],
    Input("geofence-table", "data"),
    prevent_initial_call=True,
)
def update_dropdown_options(table_data):
    """Update dropdown options based on available table columns."""
    if not table_data:
        return [], [], []

    # Get all column names from the first row
    if table_data:
        columns = list(table_data[0].keys())

        # Create options for dropdowns
        text_columns = [
            col
            for col in columns
            if col not in ["date_diff", "quantity", "exit_heading"]
        ]
        numeric_columns = [
            col for col in columns if col in ["date_diff", "quantity", "exit_heading"]
        ]
        all_columns = columns

        text_options = [
            {"label": col.replace("_", " ").title(), "value": col}
            for col in text_columns
        ]
        numeric_options = [
            {"label": col.replace("_", " ").title(), "value": col}
            for col in numeric_columns
        ]
        all_options = [
            {"label": col.replace("_", " ").title(), "value": col}
            for col in all_columns
        ]

        return all_options, numeric_options + text_options, text_options

    return [], [], []


@app.callback(
    Output("dynamic-chart", "figure"),
    [
        Input("generate-chart-btn", "n_clicks"),
        Input("chart-type-dropdown", "value"),
        Input("x-axis-dropdown", "value"),
        Input("y-axis-dropdown", "value"),
        Input("aggregation-dropdown", "value"),
        Input("color-dropdown", "value"),
    ],
    State("geofence-table", "data"),
    prevent_initial_call=True,
)
def generate_dynamic_chart(
    n_clicks, chart_type, x_axis, y_axis, aggregation, color_by, table_data
):
    """Generate dynamic charts based on user selections."""
    if not n_clicks or not table_data or not x_axis:
        return go.Figure()

    df = pd.DataFrame(table_data)

    # Handle empty dataframe
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available for plotting",
            showarrow=False,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
        )

    try:
        fig = go.Figure()

        if chart_type == "bar":
            # Perform aggregation based on user selection
            if y_axis and aggregation != "count":
                # Aggregate numeric column with specified function
                if color_by:
                    agg_func = getattr(
                        df.groupby([x_axis, color_by])[y_axis], aggregation
                    )
                    grouped = agg_func().reset_index()
                    for color_val in grouped[color_by].unique():
                        data = grouped[grouped[color_by] == color_val]
                        fig.add_trace(
                            go.Bar(
                                x=data[x_axis],
                                y=data[y_axis],
                                name=str(color_val),
                                hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                                + f"<b>{aggregation.title()} of {y_axis.replace('_', ' ').title()}</b>: %{{y}}<br>"
                                + f"<b>{color_by.replace('_', ' ').title()}</b>: {color_val}<extra></extra>",
                            )
                        )
                else:
                    agg_func = getattr(df.groupby(x_axis)[y_axis], aggregation)
                    grouped = agg_func().reset_index()
                    fig.add_trace(
                        go.Bar(
                            x=grouped[x_axis],
                            y=grouped[y_axis],
                            hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                            + f"<b>{aggregation.title()} of {y_axis.replace('_', ' ').title()}</b>: %{{y}}<extra></extra>",
                        )
                    )
            else:
                # Count occurrences (default behavior)
                if color_by:
                    grouped = (
                        df.groupby([x_axis, color_by]).size().reset_index(name="count")
                    )
                    for color_val in grouped[color_by].unique():
                        data = grouped[grouped[color_by] == color_val]
                        fig.add_trace(
                            go.Bar(
                                x=data[x_axis],
                                y=data["count"],
                                name=str(color_val),
                                hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                                + f"<b>Count</b>: %{{y}}<br>"
                                + f"<b>{color_by.replace('_', ' ').title()}</b>: {color_val}<extra></extra>",
                            )
                        )
                else:
                    counts = df[x_axis].value_counts()
                    fig.add_trace(
                        go.Bar(
                            x=counts.index,
                            y=counts.values,
                            hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                            + f"<b>Count</b>: %{{y}}<extra></extra>",
                        )
                    )

        elif chart_type == "line":
            # Perform aggregation for line charts
            if y_axis and aggregation != "count":
                if color_by:
                    agg_func = getattr(
                        df.groupby([x_axis, color_by])[y_axis], aggregation
                    )
                    grouped = agg_func().reset_index()
                    for color_val in grouped[color_by].unique():
                        data = grouped[grouped[color_by] == color_val]
                        fig.add_trace(
                            go.Scatter(
                                x=data[x_axis],
                                y=data[y_axis],
                                mode="lines+markers",
                                name=str(color_val),
                                hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                                + f"<b>{aggregation.title()} of {y_axis.replace('_', ' ').title()}</b>: %{{y}}<br>"
                                + f"<b>{color_by.replace('_', ' ').title()}</b>: {color_val}<extra></extra>",
                            )
                        )
                else:
                    agg_func = getattr(df.groupby(x_axis)[y_axis], aggregation)
                    grouped = agg_func().reset_index()
                    fig.add_trace(
                        go.Scatter(
                            x=grouped[x_axis],
                            y=grouped[y_axis],
                            mode="lines+markers",
                            hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                            + f"<b>{aggregation.title()} of {y_axis.replace('_', ' ').title()}</b>: %{{y}}<extra></extra>",
                        )
                    )
            else:
                # Count occurrences
                if color_by:
                    grouped = (
                        df.groupby([x_axis, color_by]).size().reset_index(name="count")
                    )
                    for color_val in grouped[color_by].unique():
                        data = grouped[grouped[color_by] == color_val]
                        fig.add_trace(
                            go.Scatter(
                                x=data[x_axis],
                                y=data["count"],
                                mode="lines+markers",
                                name=str(color_val),
                                hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                                + f"<b>Count</b>: %{{y}}<br>"
                                + f"<b>{color_by.replace('_', ' ').title()}</b>: {color_val}<extra></extra>",
                            )
                        )
                else:
                    counts = df[x_axis].value_counts()
                    fig.add_trace(
                        go.Scatter(
                            x=counts.index,
                            y=counts.values,
                            mode="lines+markers",
                            hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                            + f"<b>Count</b>: %{{y}}<extra></extra>",
                        )
                    )

        elif chart_type == "scatter":
            if y_axis:
                hover_text = (
                    f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                    + f"<b>{y_axis.replace('_', ' ').title()}</b>: %{{y}}<br>"
                )
                if color_by:
                    hover_text += f"<b>{color_by.replace('_', ' ').title()}</b>: %{{text}}<extra></extra>"
                else:
                    hover_text += "<extra></extra>"

                fig.add_trace(
                    go.Scatter(
                        x=df[x_axis],
                        y=df[y_axis],
                        mode="markers",
                        marker=dict(
                            color=df[color_by] if color_by else None,
                            colorscale="viridis",
                            showscale=True if color_by else False,
                        ),
                        text=df[color_by] if color_by else None,
                        hovertemplate=hover_text,
                    )
                )

        elif chart_type == "pie":
            counts = df[x_axis].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=counts.index,
                    values=counts.values,
                    hovertemplate="<b>%{label}</b><br>"
                    + "Count: %{value}<br>"
                    + "Percentage: %{percent}<extra></extra>",
                )
            )

        elif chart_type == "box":
            if y_axis:
                if color_by:
                    for color_val in df[color_by].unique():
                        data = df[df[color_by] == color_val]
                        fig.add_trace(
                            go.Box(
                                x=data[x_axis],
                                y=data[y_axis],
                                name=str(color_val),
                                hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                                + f"<b>{y_axis.replace('_', ' ').title()}</b>: %{{y}}<br>"
                                + f"<b>{color_by.replace('_', ' ').title()}</b>: {color_val}<extra></extra>",
                            )
                        )
                else:
                    fig.add_trace(
                        go.Box(
                            x=df[x_axis],
                            y=df[y_axis],
                            hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                            + f"<b>{y_axis.replace('_', ' ').title()}</b>: %{{y}}<extra></extra>",
                        )
                    )

        elif chart_type == "histogram":
            fig.add_trace(
                go.Histogram(
                    x=df[x_axis],
                    nbinsx=20,
                    hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br>"
                    + "<b>Count</b>: %{y}<extra></extra>",
                )
            )

        # Update layout
        y_title = f"{aggregation.title()}"
        if y_axis and aggregation != "count":
            y_title += f" of {y_axis.replace('_', ' ').title()}"

        fig.update_layout(
            title=f"{chart_type.title()} Chart: {x_axis.replace('_', ' ').title()}"
            + (f" by {y_title}" if y_axis or aggregation else ""),
            xaxis_title=x_axis.replace("_", " ").title(),
            yaxis_title=y_title,
            showlegend=True if color_by else False,
            height=500,
        )

        return fig

    except Exception as e:
        return go.Figure().add_annotation(
            text=f"Error generating chart: {str(e)}",
            showarrow=False,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
        )


if __name__ == "__main__":
    app.run_server(debug=False)
