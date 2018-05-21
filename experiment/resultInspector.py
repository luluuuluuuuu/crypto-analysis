import numpy as np

class ResultInspector:
    def __init__(self, trainingInputs, trainingLabels, testInputs, testLabels, model):
        self.trainingInputs = trainingInputs
        self.trainingLabels = trainingLabels
        self.testInputs = testInputs
        self.testLabels = testLabels
        self.model = model

    def getAccuracy(self):
        topK = 1

        dataSets = {
            "training": {
                "inputs": self.trainingInputs,
                "labels": self.trainingLabels
            },
            "test": {
                "inputs": self.testInputs,
                "labels": self.testLabels
            }
        }

        for key, value in dataSets.items():
            vars()[key + "Predicted"] = self.model.predict(value.get("inputs"))
            vars()[key + "PredictedClasses"] = np.fliplr(vars()[key + "Predicted"].argsort(axis=1))[:,:topK]
            vars()[key + "ActualClasses"] = np.argmax(np.array(value.get("labels")), axis=1)
            vars()[key + "Predictions"] = []
            for i, x in enumerate(vars()[key + "ActualClasses"]):
                vars()[key + "Predictions"].append(x in vars()[key + "PredictedClasses"][i])
            print(key, "Accuracy:", vars()[key + "Predictions"].count(True)/len(vars()[key + "Predictions"]))