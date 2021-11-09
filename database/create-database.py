import basedosdados as bd
import geopandas as gpd
import pandas as pd
import pymysql
import getpass
import sqlalchemy 
from shapely import wkt

# references: 
# 1) https://stackoverflow.com/questions/10154633/load-csv-data-into-mysql-in-python
# 2) https://dev.mysql.com/doc/refman/8.0/en/populating-spatial-columns.html
# 3) https://www.dataquest.io/blog/sql-insert-tutorial


# reading the json file and dropping the 'description' column, which contains the cities names
geometries = gpd.read_file('database/brazil-geometries.json')
geometries = geometries.drop(['description'], axis=1)


# turns the geometry into text (so that we can insert the data into the table)
geometries['geometry'] = geometries['geometry'].apply(wkt.dumps)


# query from Base dos Dados containing the city id, state and region of each city
state_region = bd.read_sql(
    '''
    SELECT DISTINCT id_municipio, sigla_uf, regiao
    FROM basedosdados.br_bd_diretorios_brasil.municipio
    ''',
    billing_project_id = 'analise-eleitoral-330723'
)


# renames the id, name and geometry columns
geometries = geometries.rename(columns = {'id': 'id_municipio', 'name': 'nome', 'geometry':'geometria'})


# merges the geometry and state_region dataframes, getting all the information required into one
geometries = pd.merge(state_region, geometries, on='id_municipio')
geometries.head()


# connects to local MySQL
p = getpass.getpass()
connection = pymysql.connect(host = 'localhost', port=3306, user='root', passwd=p)
cursor = connection.cursor()


# changes the SQL mode
cursor.execute('''SET @@global.sql_mode= ''; ''')


# to reverse the code above:
# cursor.execute('''SET @@global.sql_mode= 'STRICT_TRANS_TABLES'; ''')


# creates the database 'analise_eleitoral' and starts using it
cursor.execute(''' CREATE DATABASE IF NOT EXISTS analise_eleitoral ''')
cursor.execute(''' USE analise_eleitoral ''')


# creates the table 'municipalities' into the 'analise_eleitoral' database
cursor.execute(''' CREATE TABLE IF NOT EXISTS municipalities (id_municipio integer primary key, sigla_uf text, regiao text, nome text, geometria polygon) ''')


# creates an engine that connects to the database through SQLAlchemy so that we can fill the table with the data
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                              pw=p,
                              db="analise_eleitoral"))


# fills the 'municipalities' data with the data contained on the geometry dataframe
geometries.to_sql('municipalities', engine, if_exists='replace', index=False)


# commits changes
connection.commit()


# an example of query
query = "SELECT * FROM municipalities WHERE nome IN ('Rio de Janeiro', 'São Paulo', 'Belo Horizonte', 'Espírito Santo');"
df = pd.read_sql(query, con=connection)
df


# closes cursor and connection
cursor.close()
connection.close()