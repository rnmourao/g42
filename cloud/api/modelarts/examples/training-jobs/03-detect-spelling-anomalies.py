import numpy as np
from scipy.spatial.distance import pdist, squareform
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
import pandas as pd
import random
import os
import moxing as mox
import requests


# Function to find the length of the longest common subsequence of
# sequences `X[0…m-1]` and `Y[0…n-1]`
def lcs(X, Y, m, n):
 
    # return if the end of either sequence is reached
    if m == 0 or n == 0:
        return 0
 
    # if the last character of `X` and `Y` matches
    if X[m - 1] == Y[n - 1]:
        return lcs(X, Y, m - 1, n - 1) + 1
 
    # otherwise, if the last character of `X` and `Y` don't match
    return max(lcs(X, Y, m, n - 1), lcs(X, Y, m - 1, n))


# Function that inverts the lcs metric to be a distance metric
def custom_metric(x, y):
    lx = len(x)
    ly = len(y)
    d_ = lcs(x, y, lx, ly)
    maxl = max(lx, ly)
    return maxl - d_


if __name__ == "__main__":

    # data
    # data_url = 'samples'
    data_url = 'obs://roberto-public-read/modelarts-annottator-integration/data'

    mox.file.copy_parallel(data_url, '/tmp/data')

    # get the data
    df = pd.read_csv('/tmp/data/kindness.csv', header=None)

    # precompute the distances
    X = squareform(pdist(df[1].to_numpy().reshape(-1,1), lambda x, y: custom_metric(x[0], y[0])))

    # execute clustering
    dbs = DBSCAN(metric='precomputed')
    dbs.fit(X)

    # get groups
    df['evaluation'] = dbs.labels_
    df = df[[1, 'evaluation']]

    # save the results
    df.to_csv('/tmp/data/evaluation.csv', index=False, header=None)

    mox.file.copy_parallel('/tmp/data', data_url)
