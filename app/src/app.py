#****************************** Production Code ******************************#
import flask
import dash
from main import *
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
server = flask.Flask(__name__)

languages = sql.run_query("SELECT language from languages_data")
language_indicator = languages['language'].unique()
languages = sql.run_query("SELECT city from cities_data")
city_indicator = languages['city'].unique()
df = language_breakdown("a13ks3y")

colors = {
        'background': '#212121ff',
        'text': '#ffab40'
    }

# Restful API
@server.route('/api/')
def index():
    return 'API functions:'

@server.route('/api/<users>')
def hello_name(name):
   return 'Hello %s!' % users

# Dash App Functions
app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    server=server,
    routes_pathname_prefix='/'
)

def generate_table1(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

def generate_table2(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1('AutoRecuit',style={'text-align': 'center', 'background':colors['background'],'color':colors['text']}),
    html.Div([
        html.Div([
            dcc.Input(
                id='input-box',
                type='text',
                value=''
            ),
        ],
        style={'width': '25', 'margin-right': '5px'; 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='languages',
                options=[{'label': i, 'value': i} for i in language_indicator],
                value='java'
            ),
        ],
        style={'width': '25', 'margin-right': '5px'; 'display': 'inline-block'}),
        html.Div([
            dcc.Dropdown(
                id='location',
                options=[{'label': i, 'value': i} for i in city_indicator],
                value='seattle'
            ),
        ],
        style={'width': '25', 'margin-right': '5px'; 'display': 'inline-block'}),
        html.Div([
            html.Button('Submit', id='button'),
        ],
        style={'width': '25', 'margin-right': '5px'; 'display': 'inline-block'}),
    ]),
    # dcc.Graph(id='language-table'),

    html.Div(id='output-container-button',
             children='Enter a value and press submit'),

    html.H4(children='Programming Languages'),
    generate_table1(df)
    # generate_table2()
])

@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
#
#     Output('indicator-graphic', 'figure'),
#     [Input('xaxis-column', 'value'),
#      Input('yaxis-column', 'value'),
#      Input('xaxis-type', 'value'),
#      Input('yaxis-type', 'value'),
#      Input('year--slider', 'value')])
#
def update_output(n_clicks, user_name):
    str(user_name)
    df = language_breakdown(str(user_name))
    generate_table1(df)
    return 'The input value was "{}" and the button has been clicked {} times'.format(
        user_name,
        n_clicks
    )
#
# def update_graph(xaxis_column_name, yaxis_column_name,
#                  xaxis_type, yaxis_type,
#                  year_value):
#     dff = df[df['Year'] == year_value]
#
#     return {
#         'data': [dict(
#             x=dff[dff['Indicator Name'] == xaxis_column_name]['Value'],
#             y=dff[dff['Indicator Name'] == yaxis_column_name]['Value'],
#             text=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name'],
#             mode='markers',
#             marker={
#                 'size': 15,
#                 'opacity': 0.5,
#                 'line': {'width': 0.5, 'color': 'white'}
#             }
#         )],
#         'layout': dict(
#             xaxis={
#                 'title': Languages,
#                 'type': 'linear'
#             },
#             yaxis={
#                 'title': Bytes,
#                 'type': 'linear' if yaxis_type == 'Linear' else 'log'
#             },
#             margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
#             hovermode='closest'
#         )
#     }
#
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
