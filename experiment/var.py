import psycopg2 as pg
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from tslearn.metrics import dtw
from tslearn.clustering import TimeSeriesKMeans
from sklearn.decomposition import PCA
from tslearn.preprocessing import TimeSeriesScalerMeanVariance, TimeSeriesResampler

# SPLITDATE = "2018-01-01"
# windowLen = 30

SEED = 1990
N_CLUSTERS = 9
N_COMPONENTS = 44
N_TEST_DATA = 200
VAR_PERCENTILE_LEVEL = 5
METRIC = "dtw"

connection = pg.connect(database="postgres", user="postgres", password="Crypto01", host="localhost", port=5430)
cur = connection.cursor()

cur.execute("SELECT * FROM input.daily_changes ORDER BY date DESC LIMIT 500")
data = sorted(cur.fetchall(), key=lambda row: row[0], reverse=False)
daily_changes = pd.DataFrame(data=data, columns=[desc[0] for desc in cur.description], index=[row[0] for row in data], dtype="float64")
daily_changes = daily_changes.drop(["date"], axis=1)
daily_changes = daily_changes - 1

transpose_df = daily_changes.transpose()

training_data = np.array(transpose_df)
# training_data = TimeSeriesScalerMeanVariance().fit_transform(training_data)
# training_data = TimeSeriesResampler(sz=100).fit_transform(training_data)
training_data = PCA(n_components=N_COMPONENTS).fit_transform(training_data)
dba_km = TimeSeriesKMeans(n_clusters=N_CLUSTERS, max_iter=100, metric=METRIC, verbose=True, max_iter_barycenter=10, random_state=SEED)
pred_clusters = dba_km.fit_predict(training_data)

cryptos = transpose_df.index

training_clusterList = {}
training_varList = {}
test_clusterList = {}
test_varList = {}

training_list = (daily_changes.iloc[N_TEST_DATA:, :]).mean(axis=1)
training_var = np.percentile(np.array(training_list), VAR_PERCENTILE_LEVEL)
test_list = (daily_changes.iloc[:N_TEST_DATA, :]).mean(axis=1)

for i in set(pred_clusters):
    training_clusterList[i] = (daily_changes.iloc[N_TEST_DATA:, np.where(pred_clusters == i)[0]]).mean(axis=1)
    training_varList[i] = np.percentile(np.array(training_clusterList[i]), VAR_PERCENTILE_LEVEL)
    test_clusterList[i] = (daily_changes.iloc[:N_TEST_DATA, np.where(pred_clusters == i)[0]]).mean(axis=1)
    test_varList[i] = np.percentile(np.array(test_clusterList[i]), VAR_PERCENTILE_LEVEL)

before = 1 - len(test_list[test_list < training_var])/N_TEST_DATA
after = {}

for i in set(pred_clusters):
    after[i] = 1 - len(test_clusterList[i][test_clusterList[i] < training_varList[i]])/N_TEST_DATA

print("Before :", before)
print("After :", after)

for cluster in set(pred_clusters):
    print("Cluster", cluster, ":", list(cryptos[np.where(pred_clusters == cluster)]))

## Plot
for yi in set(pred_clusters):
    plt.subplot(N_CLUSTERS/3 + 1, 3, yi + 1)
    for xx in training_data[pred_clusters == yi]:
        plt.plot(xx.ravel(), "k-", alpha=.2)
    plt.plot(dba_km.cluster_centers_[yi].ravel(), "r-")
    plt.xlim(0, training_data.shape[1])
    plt.ylim(-2, 2)
    if yi == 1:
        plt.title("DTW $K$-means")

plt.tight_layout()
plt.show()
