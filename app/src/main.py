from PostgresReader.Reader import *
sql = Reader()

def language_breakdown(user_name):
    print(user_name)
    return sql.run_query("SELECT login, language, SUM(bytes) as sum \
                          FROM pie_chart_data \
                          WHERE login = '"+user_name+"'  \
                          GROUP BY login, language \
                          ORDER BY sum desc"
                          )

def projects_breakdown(user_name):
    print(user_name)
    return sql.run_query("SELECT login, project_name, language, SUM(bytes) as sum \
                          FROM pie_chart_data \
                          WHERE login = '"+user_name+"'  \
                          GROUP BY  login, project_name, language \
                          ORDER BY  sum desc
                          "
                          )

df = sql.run_query("SELECT * from pie_chart_data where login = 'abarth'")
# languages = sql.run_query("SELECT language from languages_data")
