from PostgresReader.Reader import *
# sql = Reader()
class Queries():
    def run_custom_query(self, query):
        sql = Reader()
        return sql.run_query(query)

    def language_breakdown(self, user_name):
        sql = Reader()
        return sql.run_query("SELECT login, language, SUM(bytes) as sum \
                              FROM pie_chart_data \
                              WHERE login = '"+user_name+"'  \
                              GROUP BY login, language \
                              ORDER BY (sum) desc \
                              LIMIT 6"
                              )

    def projects_breakdown_1(self, user_name):
        sql = Reader()
        return sql.run_query("SELECT a.project_name, a.description, a.language, a.bytes as sum \
                                FROM pie_chart_data a \
                                INNER JOIN ( \
                                            SELECT project_name, max(bytes) as sum \
                                            FROM pie_chart_data \
                                            WHERE login = '"+user_name+"' \
                                            GROUP BY project_name \
                                ) b  ON a.project_name = b.project_name AND a.bytes = b.sum \
                                ORDER BY a.bytes desc \
                                LIMIT 6")

    def projects_breakdown_2(self, user_name, language):
        sql = Reader()
        return sql.run_query("SELECT DISTINCT a.project_name, a.description, a.language, a.bytes as sum \
                                FROM pie_chart_data a \
                                INNER JOIN ( \
                                            SELECT project_name, max(bytes) as sum \
                                            FROM pie_chart_data \
                                            WHERE login = '"+user_name+"' AND language = '"+language+"' \
                                            GROUP BY project_name \
                                ) b  ON a.project_name = b.project_name AND a.bytes = b.sum  \
                                ORDER BY a.bytes desc \
                                LIMIT 6")

    def commits(self, user_name):
        sql = Reader()
        return sql.run_query("SELECT row_number  \
                                FROM commits_users_data a \
                                INNER JOIN ( \
                                            SELECT Login, ROW_NUMBER() OVER (ORDER BY count_of_commits)\
                                            FROM commits_users_data \
                                ) b  ON a.Login = b.Login \
                                where a.login = '"+user_name+"' ")

    def total_commits(self, user_name):
        sql = Reader()
        return sql.run_query("SELECT count(*) as count \
                               FROM commits_users_data")

    def bytes(self, user_name):
        sql = Reader()
        return sql.run_query("SELECT row_number  \
                                FROM projects_sum a \
                                INNER JOIN ( \
                                            SELECT Login, ROW_NUMBER() OVER (ORDER BY sum)\
                                            FROM projects_sum \
                                ) b  ON a.Login = b.Login \
                                where a.login = '"+user_name+"' ")

    def total_bytes(self, user_name):
        sql = Reader()
        return sql.run_query("SELECT count(*) as count \
                               FROM projects_sum")
