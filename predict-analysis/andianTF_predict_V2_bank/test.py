#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
@Date    : 2019/11/11
@Author  : Rezero
'''

import numpy as np
import matplotlib.pyplot as plt

path = "xxxx"  # 数据集路径

series = np.loadtxt(path)
series = series - np.mean(series)  # 中心化(非必须)

# step1 嵌入
windowLen = 20  # 嵌入窗口长度
seriesLen = len(series)  # 序列长度
K = seriesLen - windowLen + 1
X = np.zeros((windowLen, K))
for i in range(K):
    X[:, i] = series[i:i + windowLen]

# step2: svd分解， U和sigma已经按升序排序
U, sigma, VT = np.linalg.svd(X, full_matrices=False)

for i in range(VT.shape[0]):
    VT[i, :] *= sigma[i]
A = VT

# 重组
rec = np.zeros((windowLen, seriesLen))
for i in range(windowLen):
    for j in range(windowLen - 1):
        for m in range(j + 1):
            rec[i, j] += A[i, j - m] * U[m, i]
        rec[i, j] /= (j + 1)
    for j in range(windowLen - 1, seriesLen - windowLen + 1):
        for m in range(windowLen):
            rec[i, j] += A[i, j - m] * U[m, i]
        rec[i, j] /= windowLen
    for j in range(seriesLen - windowLen + 1, seriesLen):
        for m in range(j - seriesLen + windowLen, windowLen):
            rec[i, j] += A[i, j - m] * U[m, i]
        rec[i, j] /= (seriesLen - j)

rrr = np.sum(rec, axis=0)  # 选择重构的部分，这里选了全部

plt.figure()
for i in range(10):
    ax = plt.subplot(5, 2, i + 1)
    ax.plot(rec[i, :])

plt.figure(2)
plt.plot(series)
plt.show()
