import basedosdados as bd
import geopandas as gpd
import pandas as pd
from shapely import wkt
import pymysql
import getpass
from sqlalchemy import create_engine

# reference https://stackoverflow.com/questions/10154633/load-csv-data-into-mysql-in-python
# https://dev.mysql.com/doc/refman/8.0/en/populating-spatial-columns.html
# https://www.dataquest.io/blog/sql-insert-tutorial

p = getpass.getpass()
connection = pymysql.connect(host = 'localhost', port=3306, user='root', passwd=p, db='analise_eleitoral')
cursor = connection.cursor()

geometries = gpd.read_file('brazil-geometries.json')
geometries = geometries.drop(['description'], axis=1)

geometries['geometry'] = geometries['geometry'].apply(wkt.dumps)

state_region = bd.read_sql(
    '''
    SELECT DISTINCT id_municipio, sigla_uf, regiao
    FROM basedosdados.br_bd_diretorios_brasil.municipio
    ''',
    billing_project_id = 'analise-eleitoral-330723'
)

geometries = geometries.rename(columns = {'id': 'id_municipio', 'nome': 'name', 'geometria':'geometry'})
geometries = pd.merge(state_region, geometries, on='id_municipio')
geometries.head()


cursor.execute(''' CREATE TABLE municipalities (id_municipio integer primary key, sigla_uf text, regiao text, nome text, geometria POLYGON) ''')

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw=p,
                               db="analise_eleitoral"))

geometries.to_sql('municipalities', engine, if_exists='replace', index=False)
