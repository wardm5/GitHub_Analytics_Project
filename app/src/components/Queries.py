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
        return sql.run_query("SELECT a.project_name, a.description, a.language, a.bytes as sum                  \
                                FROM pie_chart_data a                                                           \
                                INNER JOIN (                                                                    \
                                            SELECT project_name, max(bytes) as sum                              \
                                            FROM pie_chart_data                                                 \
                                            WHERE login = '"+user_name+"'                                       \
                                            GROUP BY project_name                                               \
                                ) b  ON a.project_name = b.project_name AND a.bytes = b.sum                     \
                                ORDER BY a.bytes desc                                                              \
                                LIMIT 10")

    # def commits(self, user_name):
    #     return sql.run_query("SELECT count(*)                  \
    #                             FROM commits_users_data a                                                           \
    #                             INNER JOIN (                                                                    \
    #                                         SELECT count_of_commits                              \
    #                                         FROM commits_users_data                                                 \
    #                                         WHERE login = '"+user_name+"'                                       \
    #                             ) b  ON a.project_name = b.project_name AND a.bytes = b.sum                     \
    #                             where count_of_commits < b                                                             \
    #                             LIMIT 10")

    def commits(self, user_name):
        return sql.run_query("SELECT row_number                                                                 \
                                FROM commits_users_data a                                                       \
                                INNER JOIN (                                                                    \
                                            SELECT Login, ROW_NUMBER() OVER (ORDER BY count_of_commits)         \
                                            FROM commits_users_data                                              \
                                ) b  ON a.Login = b.Login                                                       \
                                where a.login =     '"+user_name+"' ")

    def total_commits(self, user_name):
        return sql.run_query("SELECT count(*) as count \
                               FROM commits_users_data")
