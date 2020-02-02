#****************************** Production Code ******************************#
import flask
import dash
from main import *
import dash_html_components as html
import dash_core_components as dcc
import dash_table
import pandas as pd

server = flask.Flask(__name__)

# Restful API
@server.route('/api/')
def index():
    return 'API functions:'

# Dash App Functions
app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/'
)

df = run_query()
app.layout = dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict('records'),
)


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
