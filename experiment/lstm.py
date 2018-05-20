import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from keras.layers.core import Dense, Activation, Dropout
from keras.layers.recurrent import LSTM
from keras.models import Sequential
from sklearn.metrics import log_loss
import time

SPLITDATE = "2018-01-01"
windowLen = 10

df = pd.read_csv("daily_changes.csv", sep=",")[["date", "BTC", "ETH", "DOGE", "LTC", "XRP"]]

trainingSet, testSet = df[df["date"] < SPLITDATE], df[df["date"] >= SPLITDATE]
trainingSet = trainingSet.drop(["date"], 1)
testSet = testSet.drop(["date"], 1)

trainingLabels = trainingSet.copy()[1 + windowLen:]
testLabels = testSet.copy()[1 + windowLen:]

trainingSet = trainingSet[:trainingSet.shape[0] - 1]
testSet = testSet[:testSet.shape[0] - 1]

trainingLabels = trainingLabels.assign(max=df.max(axis=1))
testLabels = testLabels.assign(max=df.max(axis=1))

columns = list(filter(lambda x: x not in ("max", "date"), df.columns))
for column in columns:
    trainingLabels.loc[trainingLabels[column] != trainingLabels["max"], column] = 0
    trainingLabels.loc[trainingLabels[column] == trainingLabels["max"], column] = 1
    testLabels.loc[testLabels[column] != testLabels["max"], column] = 0
    testLabels.loc[testLabels[column] == testLabels["max"], column] = 1

trainingLabels = trainingLabels.drop(["max"], 1).applymap(lambda x: int(x))
testLabels = testLabels.drop(["max"], 1).applymap(lambda x: int(x))

trainingInputs = []
for i in range(len(trainingSet) - windowLen):
    tmpSet = trainingSet[i:(i + windowLen)].copy()

    for col in list(tmpSet.columns):
        tmpSet[col] = tmpSet[col]/tmpSet[col].iloc[0] - 1

    trainingInputs.append(tmpSet)

trainingInputs = [np.array(trainingInput) for trainingInput in trainingInputs]
trainingInputs = np.array(trainingInputs)

testInputs = []
for i in range(len(testSet) - windowLen):
    tmpSet = testSet[i:(i + windowLen)].copy()

    for col in list(tmpSet.columns):
        tmpSet[col] = tmpSet[col]/tmpSet[col].iloc[0] - 1

    testInputs.append(tmpSet)

testInputs = [np.array(testInput) for testInput in testInputs]
testInputs = np.array(testInputs)

# Model Building

model = Sequential()

model.add(LSTM(
    units=512,
    input_shape=(trainingInputs.shape[1], trainingInputs.shape[2])
))
model.add(Dropout(0.2))

model.add(Dense(units=trainingLabels.shape[1], activation="softmax"))

model.summary()

start = time.time()
model.compile(
    loss="categorical_crossentropy",
    optimizer="rmsprop",
    metrics=["accuracy"]
)
print("compilation time : ", time.time() - start)

model.fit(
    trainingInputs,
    trainingLabels,
    batch_size=50,
    nb_epoch=10,
    validation_split=0.05
)

# Accuracy

topK = 1

training_predicted = model.predict(trainingInputs)
training_predicted_classes = np.fliplr(training_predicted.argsort(axis=1))[:, :topK]
training_actual_classes = np.argmax(np.array(trainingLabels), axis=1)

training_predictions = []
for i, x in enumerate(training_actual_classes):
    training_predictions.append(x in training_predicted_classes[i])

print("Training Accuracy: ", training_predictions.count(
    True)/len(training_predictions))

test_predicted = model.predict(testInputs)
test_predicted_classes = np.fliplr(test_predicted.argsort(axis=1))[:, :topK]
test_actual_classes = np.argmax(np.array(testLabels), axis=1)

test_predictions = []
for i, x in enumerate(test_actual_classes):
    test_predictions.append(x in test_predicted_classes[i])

print("Test Accuracy: ", test_predictions.count(True)/len(test_predictions))

# Ploting

plt.scatter(x=range(training_actual_classes[:10].shape[0]), y=training_actual_classes[:10], label="actual")
plt.scatter(x=range(training_predicted_classes[:10].shape[0]), y=[c[0] for c in training_predicted_classes[:10, 0:]], label="predicted")
plt.scatter(x=range(training_predicted_classes[:10].shape[0]), y=[c[0] for c in training_predicted_classes[:10, 1:]], label="predicted")
plt.scatter(x=range(training_predicted_classes[:10].shape[0]), y=[c[0] for c in training_predicted_classes[:10, 2:]], label="predicted")
plt.scatter(x=range(training_predicted_classes[:10].shape[0]), y=[c[0] for c in training_predicted_classes[:10, 3:]], label="predicted")
plt.scatter(x=range(training_predicted_classes[:10].shape[0]), y=[c[0] for c in training_predicted_classes[:10, 4:]], label="predicted")
plt.legend()
plt.show()
CEL = log_loss(trainingLabels, model.predict(trainingInputs))
print('The Cross Entropy Loss is: {}'.format(CEL))
