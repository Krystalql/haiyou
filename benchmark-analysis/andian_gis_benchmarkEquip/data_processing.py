import numpy as np


def create_criteria(data):
    data = data.astype(float)
    data = data.squeeze(0)
    data = data / data.min()
    n = data.shape[0]
    criteria = np.zeros((n, n))

    for i in range(n):
        for j in range(n):
            criteria[i][j] = data[i] / data[j]
    return criteria
