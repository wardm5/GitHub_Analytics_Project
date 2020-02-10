import dash_html_components as html
import dash_core_components as dcc
import dash_table
class Content():
    def build_table(self, x, y, yaxis_type):
        return {
            'data': [dict(
                x= x,
                y= y,
                type= 'bar'
            )],
            'layout': dict(
                xaxis={
                    'title': 'Languages',
                    'type': 'category'
                },
                yaxis={
                    'title': 'Mb of Data',
                    'type': 'linear' if yaxis_type == 'Linear' else 'log'
                },
                margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
                hovermode='closest'
            )
        }

    def build_commits_table(self, x):
        something = {
            'data': [dict(
                x= x,
                y= [],
                type= 'bar',
                orientation='h'
            )],
            'layout': dict(
                xaxis={
                    'title': 'Percentiles',
                    'type': 'linear',
                    "range": [0, 1]
                },
                yaxis={
                    'type': 'category'
                },
                margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
                hovermode='closest'
            )
        }
        return something

    def generate_table2(self, dataframe, max_rows=10):
        return html.Table(
            # Header
            [html.Tr([html.Th(col) for col in dataframe.columns])] +
            # Body
            [html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))]
        )

    def generate_table1(self, dataframe, max_rows=10):
        return html.Table(
            # Header
            [html.Tr([html.Th(col) for col in dataframe.columns])] +
            # Body
            [html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))]
        )
