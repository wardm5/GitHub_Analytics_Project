from PostgresReader.Reader import *
sql = Reader()

class Queries():
    def run_custom_query(self, query):
        return sql.run_query(query)

    def language_breakdown(self, user_name):
        print(user_name)
        return sql.run_query("SELECT login, language, SUM(bytes) as sum \
                              FROM pie_chart_data \
                              WHERE login = '"+user_name+"'  \
                              GROUP BY login, language \
                              ORDER BY sum desc \
                              LIMIT 6"
                              )

    def projects_breakdown(self, user_name):
        print(user_name)
        return sql.run_query("SELECT project_name, project_description, language, SUM(bytes) as sum \
                              FROM pie_chart_data \
                              WHERE login = '"+user_name+"' \
                              GROUP BY  project_name, description, language \
                              ORDER BY  sum desc \
                              LIMIT 10"
                              )

# df = sql.run_query("SELECT * from pie_chart_data where login = 'abarth'")
# languages = sql.run_query("SELECT language from languages_data")
