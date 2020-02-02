import flask
import dash
import dash_core_components as dcc
import dash_html_components as html


application = flask.Flask(__name__)
app = dash.Dash(__name__, server=application, url_base_pathname='/dash/')

# app.layout = html.Div(id='dash-container')

@application.route("/dash/")
def MyDashApp():
    return 'Hello from Flask!'

if __name__ == '__main__':
    app.run_server(host="0.0.0.0")

# import dash
# import dash_core_components as dcc
# import dash_html_components as html
# app = dash.Dash()
# app.layout = html.Div(
#         html.H1(children='Hello Dash')
# )
# if __name__ == '__main__':
#     app.run_server(host="0.0.0.0")
#
