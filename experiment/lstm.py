import numpy as np
import pandas as pd
from sklearn.metrics import log_loss
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from lstmModel import LSTMModel
from resultInspector import ResultInspector

SPLITDATE = "2018-01-01"
windowLen = 20

daily_changes = pd.read_csv("daily_changes.csv", sep=",")
clusters = pd.read_csv("clusters.csv", sep=",")

trainingSet, testSet = daily_changes[daily_changes["date"] < SPLITDATE], daily_changes[daily_changes["date"] >= SPLITDATE]

trainingLabels = trainingSet.copy()[1 + windowLen:]
testLabels = testSet.copy()[1 + windowLen:]
trainingSet = trainingSet[:trainingSet.shape[0] - 1]
testSet = testSet[:testSet.shape[0] - 1]

onehotEncoder = OneHotEncoder(sparse=False)
for dataset in ["trainingSet", "testSet", "trainingLabels", "testLabels"]:
    vars()[dataset] = vars()[dataset].drop(["date"], 1)
    vars()[dataset] = np.argmax(np.array(vars()[dataset]), axis=1)
    vars()[dataset] = np.array(clusters.loc[vars()[dataset], "cluster"])
    vars()[dataset] = vars()[dataset].reshape(len(vars()[dataset]), 1)
    vars()[dataset] = onehotEncoder.fit_transform(vars()[dataset])

trainingInputs = []
for i in range(trainingSet.shape[0] - windowLen):
    tmpSet = trainingSet[i:(i + windowLen)].copy()
    trainingInputs.append(tmpSet)

trainingInputs = [np.array(trainingInput) for trainingInput in trainingInputs]
trainingInputs = np.array(trainingInputs)

testInputs = []
for i in range(testSet.shape[0] - windowLen):
    tmpSet = testSet[i:(i + windowLen)].copy()
    testInputs.append(tmpSet)

testInputs = [np.array(testInput) for testInput in testInputs]
testInputs = np.array(testInputs)

# Model Building

lstmModel = LSTMModel(trainingInputs, trainingLabels.shape[1], 512)

model1 = lstmModel.buildModel()

model1.fit(
    trainingInputs,
    trainingLabels,
    batch_size=100,
    nb_epoch=30,
    validation_split=0.05
)

# Accuracy

resultInspector = ResultInspector(trainingInputs, trainingLabels, testInputs, testLabels, model1)
resultInspector.getAccuracy()