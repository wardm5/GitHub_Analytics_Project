############# PRODUCTION CODE #############
import flask
import dash
from components.Content import *
from components.Queries import *
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table
import pandas as pd

# Initializers
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
AutoRecruit_colors = {'background': '#212121ff', 'text': '#ffab40'}
server = flask.Flask(__name__)
content = Content()
queries = Queries()

languages = queries.run_custom_query("SELECT language FROM languages_data ORDER BY language asc")
language_indicator = languages['language'].unique()
cities = queries.run_custom_query("SELECT city FROM cities_data ORDER BY city asc")
city_indicator = cities['city'].unique()

# Restful API - Next Steps
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

app.layout = html.Div(children=[
    # Header
    html.H1('AutoRecruit',style={'text-align': 'center', 'padding': '10px', 'background':AutoRecruit_colors['background'],'color':AutoRecruit_colors['text']}),

    # row of inputs (user input, language dropdown, city dropdown, submit button)
    html.Div([
        html.Div([
            dcc.Input(
                style={'width': '100%'},
                id='input-box',
                placeholder='Enter a GitHub login...',
                type='text',
            )], style={'width': '40%', 'margin-right': '15px'}),
        html.Div([
            dcc.Dropdown(
                id='input-1-state',
                placeholder='Enter a language...',
                options=[{'label': i, 'value': i} for i in language_indicator],
            )
        ],style={'width': '20%', 'margin-right': '15px'}),
        html.Div([
            dcc.Dropdown(
                id='input-2-state',
                placeholder='Enter a major city...',
                options=[{'label': i, 'value': i} for i in city_indicator],
            )
        ], style={'width': '20%', 'margin-right': '15px'}),
        html.Button('Submit', style={'background-color': '#a4c2f4'},  id='button')
    ], style={'margin-right': '4%', 'margin-left': '2%', 'width': '100%', 'display':'flex'}),

    # Bar graph for language breakdown, table for project breakdown
    html.Div([
        html.Div([
            dcc.Graph(id='language-table'),
            dcc.RadioItems(
                id='yaxis-type',
                options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                value='Linear',
                labelStyle={'display': 'inline-block'}
            )
        ], style={'width': '40%', 'margin-right': '2%'}),
        html.Div([
            html.H4(children='Programming Languages'),
            dcc.Graph(id='commits-graph')
        ], style={'width': '50%', 'display':'block'})
    ], style={'margin' : '2%  2% 2% 2%', 'width': '100%', 'display':'flex'}),

    # content.generate_table2(df2)
])

# call back for changes to the language bar chart, modifies the graph based on query results of inputted user
@app.callback(
    Output('language-table', 'figure'),
    [Input('input-box', 'value'),
    Input('yaxis-type', 'value')])
def update_graph(user_name, yaxis_type):
    if (user_name==None):
        return content.build_table([],[], yaxis_type)
    else:
        df = queries.language_breakdown(user_name)
        return content.build_table(df['language'],df['sum'], yaxis_type)

# call back for changes to the language bar chart, modifies the graph based on query results of inputted user
@app.callback(
    Output('commits-graph', 'figure'),
    [Input('input-box', 'value')])
def update_commits_graph(user_name):
    if (user_name==None):
        return content.build_commits_table([])
    else:
        df1 = queries.commits(user_name)
        df2 = queries.total_commits(user_name)
        val = 0
        if (df1['row_number'].iloc[0] == None or 0):
            val = 0
        elif (df2['count'].iloc[0] == None or 0):
            val = 0
        else:
            val = float(df1['row_number'].iloc[0]) / float(df2['count'].iloc[0])
        list = []
        list.append(val)
        return content.build_commits_table(list)

# if __name__ == '__main__':
#     app.run_server(debug=True, host='0.0.0.0', port=8080)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080)
