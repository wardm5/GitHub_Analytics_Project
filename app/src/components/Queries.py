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



    # language averages for users
    def languages(self, language):
        sql = Reader()
        return sql.run_query(" 			select avg(b.sum)  as avg  \
                                    	FROM (		\
                                    		SELECT DISTINCT login, language,  Sum(bytes) as sum  \
                                    		FROM pie_chart_data        \
                                    		where language = '"+language+"'  \
                                    		group by login, language  \
                                    		order by sum asc  \
                                    	) b    ")
    # language and city averages for users
    def cities_languages(self, city_name, language):
        sql = Reader()
        return sql.run_query(" 			select avg(b.sum) as avg   \
                                    	FROM (		\
                                    		SELECT DISTINCT login, language,  Sum(bytes) as sum  \
                                    		FROM pie_chart_data        \
                                    		where language = '"+language+"' and city = '"+city_name+"'  \
                                    		group by login, language  \
                                    		order by sum asc  \
                                    	) b ")
    # user sum
    def user(self, user_name):
        sql = Reader()
        return sql.run_query(" 			select sum(bytes) as sum  \
                                        from pie_chart_data   \
                                        where login = '"+user_name+"'  ")
    # user sum for lanugage only
    def user_language(self, user_name, language):
        sql = Reader()
        return sql.run_query(" 			select sum(bytes) as sum \
                                        from pie_chart_data   \
                                        where login =  '"+user_name+"' and language = '"+language+"' ")




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

# queries = Queries()
# # total_language = queries.total_language()
# # print(total_users['count'].iloc[0])
# lang = queries.cities('Seattle', 'MohamedFAhmed')
# print(lang['row_number'].iloc[0])
