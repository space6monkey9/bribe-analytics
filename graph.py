import plotly.express as px
import pandas as pd
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dash import Dash, html, dcc, callback, Output, Input

app=Dash(__name__,
         title="Bribe Analytics",
        )

# Initialize psycopg2 connection pool
dsn = os.environ.get("DB_URL")
connection_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=15, dsn=dsn)

def get_data_as_dataframe(query, params=None):
    """Executes raw SQL and returns a Pandas DataFrame."""
    conn = None
    try:
        conn = connection_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            # rows are list of dicts, so columns auto-mapped
            return pd.DataFrame(rows)
    finally:
        if conn:
            connection_pool.putconn(conn)

def plot_bribe_amount_distribution():
    #Generates an interactive histogram of bribe amounts
    sql = "SELECT bribe_amt FROM bribe;"
    df = get_data_as_dataframe(sql)

    if df.empty :
        print("No data available for bribe amount distribution.")
        return None #return an empty figure

    fig = px.histogram(df, x="bribe_amt",
                       title="Distribution of Reported Bribe Amounts",
                       labels={'bribe_amt': 'Bribe Amount (INR)'},
                       nbins=50, 
                       template="plotly_white")
    fig.update_layout(yaxis_title="Number of Reports")
    return fig

def plot_total_bribe_amount_by_state():
    #Generates an interactive bar chart of total bribe amounts by state/UT.
    sql = """
        SELECT state_ut, SUM(bribe_amt) AS total_amount
        FROM bribe
        GROUP BY state_ut
        ORDER BY total_amount DESC;
    """
    df = get_data_as_dataframe(sql)

    if df.empty or 'state_ut' not in df.columns or 'total_amount' not in df.columns:
        print("No data available for total bribe amount by state.")
        return None

    fig = px.bar(df, x="state_ut", y="total_amount",
                 title="Total Reported Bribe Amount by State/UT",
                 labels={'state_ut': 'State/UT', 'total_amount': 'Total Bribe Amount (INR)'},
                 template="plotly_white")
    fig.update_layout(xaxis_title="State/UT", yaxis_title="Total Amount (INR)")
    return fig

def plot_bribes_over_time():
    #Generates an interactive line chart of bribe reports over time (monthly).
    sql = "SELECT doi FROM bribe WHERE doi IS NOT NULL;"
    df = get_data_as_dataframe(sql)

    if df.empty or 'doi' not in df.columns:
        print("No data with dates available for bribes over time.")
        return None

    # Ensure 'doi' is datetime type and handle potential errors
    df['doi'] = pd.to_datetime(df['doi'], errors='coerce')
    df.dropna(subset=['doi'], inplace=True) # Drop rows where conversion failed

    if df.empty:
        print("No valid dates found after conversion.")
        return None

    # Aggregate by month
    df['month_year'] = df['doi'].dt.to_period('M').astype(str) # Group by month-year string
    monthly_counts = df.groupby('month_year').size().reset_index(name='count')
    monthly_counts = monthly_counts.sort_values('month_year') # Ensure chronological order

    fig = px.line(monthly_counts, x="month_year", y="count",
                  title="Number of Bribe Reports Over Time (Monthly)",
                  labels={'month_year': 'Month', 'count': 'Number of Reports'},
                  markers=True,
                  template="plotly_white")
    fig.update_layout(xaxis_title="Month", yaxis_title="Number of Reports")
    return fig

def plot_top_departments_by_bribe_amount(top_n=15):
    #Generates an interactive bar chart of top 15 departments by total bribe amount.
    sql = """
        SELECT dept, SUM(bribe_amt) AS total_amount
        FROM bribe
        GROUP BY dept
        ORDER BY total_amount DESC
        LIMIT %s;
    """
    df = get_data_as_dataframe(sql, (top_n,))

    if df.empty or 'dept' not in df.columns or 'total_amount' not in df.columns:
        print(f"No data available for top {top_n} departments by bribe amount.")
        return None

    fig = px.bar(df, x="dept", y="total_amount",
                 title=f"Top {top_n} Departments by Total Reported Bribe Amount",
                 labels={'dept': 'Department', 'total_amount': 'Total Bribe Amount (INR)'},
                 template="plotly_white")
    fig.update_layout(xaxis_title="Department", yaxis_title="Total Amount (INR)")
    fig.update_xaxes(tickangle= -45)
    return fig

def plot_top_districts_by_bribe_amount(top_n=20):
    #Generates an interactive bar chart of top 20 districts by total bribe amount.
    sql = """
        SELECT district, SUM(bribe_amt) AS total_amount
        FROM bribe
        GROUP BY district
        ORDER BY total_amount DESC
        LIMIT %s;
    """
    df = get_data_as_dataframe(sql, (top_n,))

    if df.empty or 'district' not in df.columns or 'total_amount' not in df.columns:
        print(f"No data available for top {top_n} districts by bribe amount.")
        return None

    fig = px.bar(df, x="district", y="total_amount",
                 title=f"Top {top_n} Districts by Total Reported Bribe Amount",
                 labels={'district': 'District', 'total_amount': 'Total Bribe Amount (INR)'},
                 template="plotly_white")
    fig.update_layout(xaxis_title="District", yaxis_title="Total Amount (INR)")
    fig.update_xaxes(tickangle= -45)
    return fig

if __name__ == "__main__":

    app.layout = html.Div(
            children=[
                html.H1(
                    className="header",
                    children="Bribe Analytics Dashboard",
                    style={
                        "text-align": "center",
                        "font-size": "32px",
                        "font-family": "Arial, sans-serif",
                    },
                ),
                html.Hr(),
                html.H2(
                    className="subheader",
                    children="Interesting datapoints to explore",
                    style={
                        "text-align": "center",
                        "font-family": "Arial, sans-serif",
                        "font-weight": "500",
                    },
                ),
                html.Div(
                    children=[
                        html.Label("Graphs: "),
                        html.Div(
                            children=[
                                dcc.Dropdown(
                                    [
                                    "Bribe distribution",
                                    "State wise Bribe data",
                                    "Top 15 Departments",
                                    "Top 20 Districts",
                                    "Bribes over time",
                                ],
                                "Bribe distribution",
                                    id="dropdown",
                                )
                            ],
                            style={"width": "20%", "margin-left": "5px"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "justifyContent": "center",
                        "alignItems": "center",
                    },
                ),
                dcc.Graph(figure={}, id="graph"),
            ]
        )

    @callback(
        Output(component_id="graph", component_property="figure"),
        Input(component_id="dropdown", component_property="value"),
    )
    def update_graph(value):
        fig = None
        if value == "Bribe distribution":
            fig = plot_bribe_amount_distribution()

        elif value == "State wise Bribe data":
            fig = plot_total_bribe_amount_by_state()

        elif value == "Top 15 Departments":
            fig = plot_top_departments_by_bribe_amount()

        elif value == "Top 20 Districts":
            fig = plot_top_districts_by_bribe_amount()

        elif value == "Bribes over time":
            fig = plot_bribes_over_time()

        # Return an empty figure if no data is found
        return fig if fig is not None else {}
    
    port = int(os.environ.get("PORT", 10000))  # fallback to 10000 if PORT is unset
    app.run(host="0.0.0.0", port=port)
