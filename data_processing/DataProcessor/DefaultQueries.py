# Creates Pie Chart Graph
def create_pie_chart_data(self):
    if (self.started == None):
        return
    print("Status: Getting commit counts per project")
    projects = self.table_map['projects'].alias('projects')
    prod_lang = self.table_map['project_languages'].alias('prod_lang')
    inner_join = projects.join(prod_lang, projects.id == prod_lang.project_id) \
                    .select(projects['owner_id'], projects['url'], projects['name'] \
                    , projects['id'], prod_lang['language'], prod_lang['bytes']) \
                    .where(projects['updated_at'] = lang['created_at'] & projects['forked_from'] = None)
    inner_join = inner_join.orderBy('owner_id', 'id', inner_join['bytes'].desc())
    inner_join.show()

# Creates Language Percent for final project - FINAL QUERY, DO NOT CHANGE
def create_bar_chart_of_langauge_table(self):
    if (self.started == None):
        return
    print("Status: Getting commit counts per project")
    projects = self.table_map['projects'].groupBy('owner_id', 'language').agg(count('id').alias('count')).select('owner_id', 'language', 'count')
    projects = projects.orderBy('owner_id', projects['count'].desc())
    projects_table = projects.alias('projects_table')
    users_table = self.table_map['users'].alias('users_table')
    print("Status: joining users and projects tables")
    inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table['login'], users_table['location'],projects_table['*'])
    inner_join.show()
    self.table_map['pie_chart_table'] = inner_join

def create_default_table_3(self):
    if (self.started == None):
        return
    print("Status: Getting commit counts per project")
    projects = self.table_map['projects'].groupBy('language', 'owner_id').agg(count('id').alias('count')).select('language', 'owner_id', 'count(id)')
    projects = projects.orderBy('language', projects['count'].desc())
    window = Window.partitionBy(projects['language']).orderBy(projects['count'].desc())
    projects = projects.select('*', percent_rank().over(window).alias('rank'))
    projects_table = projects.alias('projects_table')
    users_table = self.table_map['users'].alias('users_table')
    print("Status: joining users and projects tables")
    inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table["login"],projects_table['*'])
    inner_join = inner_join.orderBy('language', 'count')
    inner_join.show()
    # print(inner_join.count())
    self.table_map['default_3'] = inner_join


def create_projects_language_filter_table(self):
    if (self.started == None):
        return
    print("Status: Getting commit counts per project")
    projects = self.table_map['projects'].groupBy('language', 'owner_id').agg(count('id').alias('count')).select('language', 'owner_id', 'count(id)')
    projects = projects.orderBy('language', projects['count'].desc())
    window = Window.partitionBy(projects['language']).orderBy(projects['count'].desc())
    projects = projects.select('*', percent_rank().over(window).alias('rank'))
    projects_table = projects.alias('projects_table')
    users_table = self.table_map['users'].alias('users_table')
    print("Status: joining users and projects tables")
    inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table["login"],projects_table['*'])
    inner_join = inner_join.orderBy('language', 'count')
    inner_join.show()
    # print(inner_join.count())
    self.table_map['default_4'] = inner_join

# Creates Percentile Table for final project
def create_default_table_1(self):
    if (self.started == None):
        return
    print("Status: Getting commit counts per user")
    commits = self.table_map['commits'].groupBy('committer_id').agg(F.count('commit_id'))
    commits = commits.orderBy(commits['count(commit_id)'].asc())
    commits.show()
    print(commits.count())
    commits_table = self.table_map['commits'].alias('commits_table')
    users_table = self.table_map['users'].alias('users_table')
    print("Status: joining users and commits tables")
    inner_join = commits.join(users_table, commits_table.committer_id == users_table.id).select(users_table["login"],commits_table["*"])
    # print("inner join table count: " , inner_join.count())
    inner_join.show()
    # self.table_map['percentiles'] = inner_join
