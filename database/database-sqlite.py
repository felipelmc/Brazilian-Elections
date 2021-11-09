import basedosdados as bd
import geopandas as gpd
import pandas as pd
from shapely import wkt
import sqlite3
from sqlalchemy import create_engine

geometries = gpd.read_file('database/brazil-geometries.json')
geometries = geometries.drop(['description'], axis=1)

geometries['geometry'] = geometries['geometry'].apply(wkt.dumps)

state_region = bd.read_sql(
    '''
    SELECT DISTINCT id_municipio, sigla_uf, regiao
    FROM basedosdados.br_bd_diretorios_brasil.municipio
    ''',
    billing_project_id = 'analise-eleitoral-330723'
)

geometries = geometries.rename(columns = {'id': 'id_municipio'})

geometries = pd.merge(state_region, geometries, on='id_municipio')
geometries.head()

# reference: https://gis.stackexchange.com/questions/346248/sqlite-error-no-such-function-initspatialmetadata

conn = sqlite3.connect('municipalities.db')

conn.enable_load_extension(True)
cursor = conn.cursor()
cursor.execute("SELECT load_extension('mod_spatialite')")
cursor.execute("SELECT InitSpatialMetaData()")

cursor.execute('CREATE TABLE municipalities (id_municipio integer primary key, sigla_uf text, regiao text, name text)')
conn.commit()

cursor.execute("SELECT AddGeometryColumn('municipalities', 'geometry', 4326, 'POLYGON', 'XY');")
cursor.execute("SELECT CreateSpatialIndex('municipalities', 'geometry');")

cursor.execute('pragma table_info(municipalities)')
cursor.fetchall()

conn.commit()

# por algum motivo fica voltando pra text ao invés de manter no formato geometry

geometries.to_sql('municipalities', conn, if_exists='replace', index=False)

query = '''
SELECT id_municipio, sigla_uf, regiao
FROM municipalities 
WHERE sigla_uf = "RJ"
'''

df = pd.read_sql_query(query, conn)
df.head()

cursor.execute('pragma table_info(municipalities)')
cursor.fetchall()

cursor.close()
conn.close()