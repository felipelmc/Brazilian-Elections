import basedosdados as bd
import geopandas as gpd
import pandas as pd
import sqlalchemy 
import getpass
import plotly.express as px
from shapely import wkt

p = getpass.getpass()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                              pw=p,
                              db="analise_eleitoral"))
conn = engine.connect()

query = ''' 
SELECT * 
FROM municipalities
'''

geometries = pd.read_sql(query, conn)
geometries.dtypes

geometries['geometria'] = gpd.GeoSeries.from_wkt(geometries['geometria'])
geometries = gpd.GeoDataFrame(geometries, geometry='geometria')
geometries.dtypes

conn.close()

data = geometries[geometries["sigla_uf"] == "RJ"].reset_index(drop=True)

fig = px.choropleth(data,
                   geojson=data.geometry,
                   locations=data.index,
                   color = "nome",
                   #projection="mercator"
                   )

fig.show()