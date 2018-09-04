from pybrain.tools.shortcuts import buildNetwork
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.structure.modules import TanhLayer

import numpy as np

def run():

    erik_nn = buildNetwork(1, 2, 1, bias="true", fast="true")

    datasize = 100

    erik_data = SupervisedDataSet(1, 1)

    sin_val = np.random.uniform(low=-1, high=1, size=datasize)
    sin_answer = np.empty(datasize)

    for i in xrange(len(sin_val)):
        sin_answer[i] = np.sin(sin_val[i])
        erik_data.addSample(sin_val[i], sin_answer[i])

    erik_trainer = BackpropTrainer(erik_nn, erik_data, learningrate=0.1, momentum=0.0, weightdecay=0.0, verbose=False)

    erik_trainer.trainEpochs(epochs=1000)

    print(erik_nn.activate([0.87]))


if __name__ == '__main__':
    run()
