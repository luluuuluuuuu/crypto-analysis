from keras.models import Sequential
from keras.layers.core import Dense, Activation, Dropout
from keras.layers.recurrent import LSTM
import time

class LSTMModel:
    def __init__(self, inputs, outputSize, neurons):
        self.inputs = inputs
        self.outputSize = outputSize
        self.neurons = neurons

    def buildModel(self, activationFunc="softmax",
                dropoutRate=0.2, loss="binary_crossentropy", optimizer="rmsprop"):
        model = Sequential()

        model.add(LSTM(
            units=self.neurons,
            input_shape=(self.inputs.shape[1], self.inputs.shape[2])
        ))
        model.add(Dropout(dropoutRate))

        model.add(Dense(units=self.outputSize, activation=activationFunc))

        model.summary()

        start = time.time()
        model.compile(
            loss=loss,
            optimizer=optimizer,
            metrics=["accuracy"]
        )
        print("compilation time : ", time.time() - start)
        return model