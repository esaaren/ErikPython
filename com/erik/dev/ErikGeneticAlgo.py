import numpy as np
import random
from random import randint


class Genetic:

    def __init__(self, popsize, stringsize, stringsplits, crossover, mutation):
        self.popsize = popsize
        self.stringsize = stringsize
        self.stringsplits = stringsplits
        self.crossover = crossover
        self.mutation = mutation
        self.population = np.chararray(self.popsize)
        self.population = np.chararray(self.population.shape, itemsize=self.stringsize)

        # Initialize the population with binary strings
        for i in xrange(0, len(self.population)):
            binary_string_temp = np.chararray(self.stringsize)
            for j in xrange(0, self.stringsize):
                bin_rand = randint(0, 1)
                binary_string_temp[j] = str(bin_rand)
            self.population[i] = ''.join(binary_string_temp)

    @staticmethod
    def crossover(rate, binary1, binary2):
        b1 = list(binary1)
        b2 = list(binary2)

        n1 = np.chararray(len(b1))
        n2 = np.chararray(len(b2))

        # random double between 0, 1
        r_crossover = random.random()
        print(r_crossover)
        # Check if the rand between 0, 1 is less than crossover (0.7), if so then crossover at a random index
        if r_crossover < rate:
            # 0 crossover before, 1 crossover after
            crossover_direction = randint(0, 1)

            crossover_index = randint(0, len(b1))

            if crossover_direction == 0:
                for i in xrange(0, crossover_index):
                    n1[i] = b1[i]
                    n2[i] = b2[i]

                    b1[i] = n2[i]
                    b2[i] = n1[i]

            if crossover_direction == 1:
                for i in xrange(crossover_index, len(b1)):
                    n1[i] = b1[i]
                    n2[i] = b2[i]

                    b1[i] = n2[i]
                    b2[i] = n1[i]

            children = np.chararray(2)
            children = np.chararray(children.shape, itemsize=len(b1))
            children[0] = ''.join(b1)
            children[1] = ''.join(b2)

            return children
        return 0

    # Calculate fitness for just y = 5
    def calculateFitness(self, binary_string):
        value = int(binary_string, 2)
        y = value + 5
        return y

    # Print out the population
    def printPopulation(self):
        for i in self.population:
            print(i)


def run():

    G = Genetic(100, 4, 0, 0.7, 0.01)

    test = Genetic.crossover(0.7, '1111', '0000')

    print(test)


if __name__ == '__main__':
    run()
