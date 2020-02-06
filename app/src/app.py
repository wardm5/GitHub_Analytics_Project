############# PRODUCTION CODE #############
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

languages = sql.run_query("SELECT language FROM languages_data ORDER BY language asc")
language_indicator = languages['language'].unique()
languages = sql.run_query("SELECT city FROM cities_data ORDER BY city asc")
city_indicator = languages['city'].unique()
df = language_breakdown("abarth")
df2 = projects_breakdown("abarth")
name = None

colors = {'background': '#212121ff', 'text': '#ffab40'}

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

app.layout = html.Div(children=[
    html.H1('AutoRecruit',style={'text-align': 'center', 'padding': '10px', 'background':colors['background'],'color':colors['text']}),

    html.Div([
        html.Div([
            dcc.Input(
                style={'width': '100%'},
                id='input-box',
                placeholder='Enter a GitHub login...',
                type='text',
                # value='testing'
            )],
            style={'width': '40%', 'margin-right': '15px'}),
        html.Div([
            dcc.Dropdown(
                id='input-1-state',
                placeholder='Enter a language...',
                options=[{'label': i, 'value': i} for i in language_indicator],
                # value='java'
            )],
            style={'width': '20%', 'margin-right': '15px'}),
        html.Div([
            dcc.Dropdown(
                id='input-2-state',
                placeholder='Enter a major city...',
                options=[{'label': i, 'value': i} for i in city_indicator],
                # value='seattle'
            )],
            style={'width': '20%', 'margin-right': '15px'}),
        html.Button('Submit', style={'background-color': '#a4c2f4'},  id='button')
    ],
    style={'margin-right': '4%', 'margin-left': '2%', 'width': '100%', 'display':'flex'}),
    # html.Div(style={'margin-left': '2%'},
    #         id='output-container-button',
    #         children='Enter a value and press submit'),

    html.Div([
        dcc.Graph(id='language-table'),
        dcc.RadioItems(
            id='yaxis-type',
            options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
            value='Linear',
            labelStyle={'display': 'inline-block'}
        )
    ], style={'margin' : '2%  2% 2% 2%', 'width': '40%', 'height': '10%', 'display':'block'}),

    html.H4(children='Programming Languages'),
    generate_table1(df),

    generate_table2(df2)
])

# @app.callback(
#     dash.dependencies.Output('output-container-button', 'children'),
#     [dash.dependencies.Input('button', 'n_clicks')],
#     [dash.dependencies.State('input-box', 'value')])
# def update_output(n_clicks, user_name):
#     # str(user_name)
#     # df = language_breakdown(str(user_name))
#     return 'The input value was "{}" and the button has been clicked {} times'.format(
#         user_name,
#         n_clicks
#     )

@app.callback(
    Output('language-table', 'figure'),
    [Input('input-box', 'value'),
    Input('yaxis-type', 'value')])
def update_graph(user_name, yaxis_type):
    if (user_name==None):
        return {
            'data': [dict(
                x= [],
                y= [],
                type= 'bar'
            )],
            'layout': dict(
                xaxis={
                    'title': 'Languages',
                    'type': 'category'
                },
                yaxis={
                    'title': 'Gb of Data',
                    'type': 'linear' if yaxis_type == 'Linear' else 'log'
                },
                margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
                hovermode='closest'
            )
        }
    df = language_breakdown(user_name)
    return {
        'data': [dict(
            x= df['language'],
            y= df['sum'],
            type= 'bar'
        )],
        'layout': dict(
            xaxis={
                'title': 'Languages',
                'type': 'category'
            },
            yaxis={
                'title': 'Gb of Data',
                'type': 'linear' if yaxis_type == 'Linear' else 'log'
            },
            margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
            hovermode='closest'
        )
    }

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
