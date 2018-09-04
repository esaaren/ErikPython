import numpy as np


def run():

    X = np.random.uniform(low=-1, high=1, size=100)
    Y = np.empty(100)

    for i in xrange(len(X)):
        Y[i] = np.sin(X[i])


    zipped = zip(X, Y)

    np.savetxt('C:\users\erik.saarenvirta\Desktop\sin_data.csv', zipped, fmt='%f,%f')

    X1 = np.random.uniform(low=-1, high=1, size=100)

    np.savetxt('C:\users\erik.saarenvirta\Desktop\sin_test.csv', X1)


if __name__ == '__main__':
    run()