import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.contrib import rnn

SPLITDATE = '2018-01-01'

df = pd.read_csv('daily_changes.csv', sep=',')

trainingSet, testSet = df[df['date'] < SPLITDATE], df[df['date'] >= SPLITDATE]

trainingSet = trainingSet.drop(['date'], 1)
testSet = testSet.drop(['date'], 1)

trainingLabels = trainingSet.copy().assign(max=df.max(axis=1))
testLabels = testSet.copy().assign(max=df.max(axis=1))

columns = list(filter(lambda x: x not in ('max', 'date'), df.columns))
for column in columns:
    trainingLabels.loc[trainingLabels[column]
                       != trainingLabels['max'], column] = 0
    trainingLabels.loc[trainingLabels[column]
                       == trainingLabels['max'], column] = 1
    testLabels.loc[testLabels[column] != testLabels['max'], column] = 0
    testLabels.loc[testLabels[column] == testLabels['max'], column] = 1

trainingLabels = trainingLabels.drop(['max'], 1).applymap(lambda x: int(x))
testLabels = testLabels.drop(['max'], 1).applymap(lambda x: int(x))

time_steps = trainingSet.shape[0]
num_units = 128
n_input = trainingSet.shape[1]
learning_rate = 0.001
n_classes = trainingLabels.shape[1]
batch_size = 128
