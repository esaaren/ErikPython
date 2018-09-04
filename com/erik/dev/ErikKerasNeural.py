from keras.models import Sequential
from keras.layers.core import Dense, Activation
from keras.optimizers import SGD
from multiprocessing import Pool
import numpy as np
import keras.callbacks


def run():

    # fix random seed for reproducibility
    seed = 7
    np.random.seed(seed)

    '''
    # Load a dataset from a CSV example
    mortgage_data = np.genfromtxt("C:\users\erik.saarenvirta\Desktop\sample_mortgage.csv", delimiter=',')

    # split into input (X) and output (Y) variables
    X = mortgage_data[:, 1:3]

    Y = mortgage_data[:, 3]
    '''

    data_len = 1000
    # Initialize X to a bunch of random values between -1 and 1 (just like sin)
    X = np.random.uniform(low=-1, high=1, size=data_len)

    # Set Y to calculate the X values using sin, this will be training set
    Y = np.empty(data_len)
    for i in xrange(len(Y)):
        Y[i] = np.sin(X[i])

    # Two layers, init (initialization) uniform sets weights to uniform values between 0 & 0.05
    # Can try setting init = normal will set to gaussian random with mu = 0 and std_dev = 0.05

    erik_nn = Sequential()
    erik_nn.add(Dense(1, activation="tanh", kernel_initializer="uniform", input_dim=1))   # 1 input neuron expecting dimension 1
    erik_nn.add(Dense(2, activation="sigmoid", kernel_initializer="uniform"))                # 2 hidden layer neurons
    erik_nn.add(Dense(1, kernel_initializer="uniform"))                   # 1 output layer neurons

    # Compile model
    # Using loss function as mean squared error, could try using others for diff. results
    erik_nn.compile(loss='mse', optimizer=SGD(lr=0.1, momentum=0.99, decay=0.001, nesterov=False))

    ''' Use this to monitor the validation_loss for not changing over a patience (# epochs)
    # Learning rate will be adjusted to LR * factor
    reduce_model_LR = keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.1, patience=10, verbose=0, mode='auto',
                                                         cooldown=0, min_lr=0.001)

    # Use this to set an early stopping
    # Absolute value less than min_delta for a patience of x will end the training
    stop_early = keras.callbacks.EarlyStopping(monitor='val_loss', min_delta=0, patience=0, verbose=0, mode='auto')

    '''

    # Fitting the model
    # Set epochs, batch size, how much % of data to use as training & shuffle the training each epoch
    # Can add      callbacks=[reduce_model_LR] into this as a parameter
    # Can add      stop_early as a callback as well
    erik_nn.fit(X, Y, epochs=1500, batch_size=400, validation_split=0.8, verbose=1)

    # Evaluate the model
    test = np.array([0.86])
    scores = erik_nn.predict(test)
    print scores

if __name__ == '__main__':
    run()