import psycopg2 as pg
import plotly as py
import plotly.graph_objs as go

connection = pg.connect(database="postgres", user="postgres", password="Crypto01", host="localhost", port=5430)

cur = connection.cursor()

cur.execute("SELECT feature0, feature1, feature2 FROM output.kmeans_centers")
rows = cur.fetchall()

x = []
y = []
z = []

for row in rows:
    x.append(row[0])
    y.append(row[1])
    z.append(row[2])

trace = go.Scatter3d(x=x, y=y, z=z, mode="markers")
py.offline.iplot([trace])