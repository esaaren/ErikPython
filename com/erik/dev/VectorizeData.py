import sys

from gensim.models import Word2Vec
from gensim.models import KeyedVectors
from sklearn.decomposition import PCA
from matplotlib import pyplot
import re

def run():

    content = "Hello world it is Erik!"

    # replace special characters with " "
    line = re.sub("[^\w\s\-\_]"," ", content)

    # change any whitespace to one space
    line = re.sub("\s+", " ", line)

    # remove start and end whitespace
    line = line.strip()

    article_token = line.lower().split()
    print "Article Token: ", article_token
    # print "Article Token: ", article_token

    # define training data
    sentences = [article_token]
    # print sentences
    # train model
    model = Word2Vec(sentences, min_count=1)
    # summarize the loaded model
    print "Model: ", model
    # summarize vocabulary
    words = list(model.wv.vocab)
    print "Words:", words
    # access vector for one word
    print(model.wv['hello'])

    # fit a 2d PCA model to the vectors
    X = model[model.wv.vocab]
    pca = PCA(n_components=2)
    result = pca.fit_transform(X)
    # create a scatter plot of the projection
    pyplot.scatter(result[:, 0], result[:, 1])
    words = list(model.wv.vocab)
    for i, word in enumerate(words):
        pyplot.annotate(word, xy=(result[i, 0], result[i, 1]))
    pyplot.show()

    #vector = model.wv[article_token]
    #print "Vector", vector
    print "End of Lambda Function"



if __name__ == "__main__":
    run()