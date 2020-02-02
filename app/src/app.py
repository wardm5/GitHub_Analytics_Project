from dash import Dash
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['/static/dist/css/style.css']
external_scripts = ['/static/dist/js/includes/jquery.min.js',
                    '/static/dist/js/main.js']


app = Dash(__name__,
          external_stylesheets=external_stylesheets,
          external_scripts=external_scripts,
          routes_pathname_prefix='/')

app.layout = html.Div(id='example-div-element')

if __name__ == '__main__':
    app.run_server(debug=True)
