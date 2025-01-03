import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import config  # Import MySQL configuration

# MySQL connection URL
DATABASE_URI = f"mysql+pymysql://{config.MYSQL_USERNAME}:{config.MYSQL_PASSWORD}@{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DB}"
engine = create_engine(DATABASE_URI)

# Initialize Dash app
app = dash.Dash(__name__)

def fetch_data():
    """Fetch data from MySQL database."""
    query = "SELECT * FROM inpatient_records"
    df = pd.read_sql(query, con=engine)
    return df

# Layout
app.layout = html.Div([
    html.H1("Healthcare Analytics Dashboard"),
    
    html.Div([
        dcc.Dropdown(
            id='region-filter',
            options=[{'label': region, 'value': region} for region in fetch_data()['region'].unique()],
            placeholder="Select a region"
        ),
        dcc.Graph(id='trend-chart'),
        dcc.Graph(id='cost-chart'),
    ]),
    
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)  # Refresh every 60 seconds
])

# Callbacks
@app.callback(
    [Output('trend-chart', 'figure'),
     Output('cost-chart', 'figure')],
    [Input('region-filter', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_charts(region, n):
    df = fetch_data()
    if region:
        df = df[df['region'] == region]

    # Line chart for disease trends
    trend_chart = {
        'data': [{'x': df['admission_date'], 'y': df['diagnosis'].value_counts(), 'type': 'line', 'name': 'Disease Trend'}],
        'layout': {'title': f'Disease Trends in {region or "All Regions"}'}
    }

    # Bar chart for cost distribution
    cost_chart = {
        'data': [{'x': df['region'], 'y': df.groupby('region')['cost'].sum(), 'type': 'bar', 'name': 'Cost by Region'}],
        'layout': {'title': 'Cost Analysis by Region'}
    }

    return trend_chart, cost_chart

if __name__ == '__main__':
    app.run_server(debug=True)
