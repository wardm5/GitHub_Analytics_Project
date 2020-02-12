############# PRODUCTION CODE #############
import flask
import dash
from components.Content import *
from components.Queries import *
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output

# Initializers
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
AutoRecruit_colors = {'background': '#212121ff', 'text': '#ffab40'}
server = flask.Flask(__name__)
content = Content()
queries = Queries()
overall_score = 0

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
    return 'Hello %s!' % user

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
            html.Div(
                [html.H6(style={'width': 'min-content'}, id="commit-percentile"), html.P("Commit Percentile")],
                style={'width': '30%',  'padding': '1%', 'background': '#d3d3d3', 'border-radius': '10px', 'margin-right': '2%'},
            ),
            html.Div(
                [html.H6(style={'width': 'min-content'},id="bytes-percentile"), html.P("Bytes Percentile")],
                style={'width': '30%', 'padding': '1%',  'background': '#d3d3d3', 'border-radius': '10px', 'margin-right': '2%'},
            ),
            html.Div(
                [html.H6(style={'width': 'min-content'},id="score"), html.P("Total Candidate Score")],
                style={'width': '30%', 'padding': '1%',  'background': '#d3d3d3', 'border-radius': '10px', 'margin-right': '2%'},
            ),
        ], style={'width': '50%', 'display':'flex', 'height': 'fit-content'})
    ], style={'margin' : '2%  2% 2% 2%', 'width': '100%', 'display':'flex'}),

    # table with project information
    html.Div(id='my-div')
])

# call back calculates user's percentiles for commits, bytes, and overall score
@app.callback(
    [dash.dependencies.Output("bytes-percentile", "children"),
     dash.dependencies.Output("commit-percentile", "children"),
     dash.dependencies.Output("score", "children")],
    [dash.dependencies.Input('button', 'n_clicks'),
     dash.dependencies.Input('input-1-state', 'value'),
     dash.dependencies.Input('input-2-state', 'value')],
    [dash.dependencies.State('input-box', 'value')])
# if inputs such as button being clicked ouccrs, then this method will take in the parameters
# and use them to calculate the overall score, commit percentile, and byte percentile for the user
def update_well_text(n_clicks, language, city, user_name):
    if (user_name == None):
        return "0.0%", "0.0%", "0.0%"
    else:
        bytes = queries.bytes(user_name)
        total_bytes = queries.total_bytes(user_name)
        commits = queries.commits(user_name)
        total_commits = queries.total_commits(user_name)
        try:
            bytes_percentile = float(bytes['row_number'].iloc[0]) / float(total_bytes['count'].iloc[0]) * 100
            bytes_percentile = round(bytes_percentile, 2)

            commits_percentile = float(commits['row_number'].iloc[0]) / float(total_commits['count'].iloc[0]) * 100
            commits_percentile = round(commits_percentile, 2)
            return str(bytes_percentile) + "%",  str(commits_percentile) + "%", str((bytes_percentile + commits_percentile) / 2) + "%"
        except:
            return "0.0%", "0.0%", "0.0%"

# call back for changes to the language bar chart, modifies the graph based on query results of inputted user
@app.callback(
    dash.dependencies.Output('language-table', 'figure'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_graph(n_clicks, user_name):
    if (user_name==None):
        return content.build_table([],[], 'linear')
    else:
        df = queries.language_breakdown(user_name)
        df['sum'] = df['sum'].div(1000000).round(1)
        return content.build_table(df['language'],df['sum'], 'linear')

# call back gets user's largest projects and displays them in a table
@app.callback(
    dash.dependencies.Output(component_id='my-div', component_property='children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output_div(n_clicks, user_name):
    if (user_name==None):
        return None
    else:
        df = queries.projects_breakdown(user_name)
        return content.generate_table2(df)

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)

# if __name__ == '__main__':
#     app.run_server(host='0.0.0.0', port=8080)
