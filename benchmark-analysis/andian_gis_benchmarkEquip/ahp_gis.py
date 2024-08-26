import numpy as np

from ahp import AHP


def gis_benchmark(dataframe, criteria, outputTime):
    dataframe.columns = ["GIS-ID", "GIS名", "放电峰值", "放电幅值", "放电次数", "压力", "微水", "温度", "标签"]

    if sum(dataframe.isna().sum()) != 0:
        dataframe["标签"] = dataframe["标签"].fillna("异常")
        dataframe = dataframe.fillna(0)

    # 更改标签，无风险为1类，其余为0类
    dataframe["标签"].loc[dataframe["标签"] == "无风险"] = "I级"
    dataframe["标签"].loc[dataframe["标签"] == "一级风险"] = "II级"
    dataframe["标签"].loc[dataframe["标签"] == "二级风险"] = "III级"
    dataframe["标签"].loc[dataframe["标签"] == "三级风险"] = "IV级"

    data = dataframe[["放电峰值", "放电幅值", "放电次数", "压力", "微水", "温度", "标签"]]

    x = data.values
    normalId = np.where(x[:, -1] == "I级")
    abnormalId = np.where(x[:, -1] != "I级")
    x = x[:, :-1].astype(float)
    if np.any(x == 0):
        print("There are zeros in the data")
        x[np.where(x == 0)] = 0.0001
    # x = np.delete(x, np.where(np.isnan(x))[0], axis=0)

    normal = x[normalId]
    abnormal = x[abnormalId]

    m = normal.shape[0]  # 正常个数
    n = abnormal.shape[0]  # 异常个数

    # 对每个准则，方案优劣排序
    # 对于正常的变压器
    if len(normal) > 1:

        b1 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b1[i, j] = normal[j, 0] / normal[i, 0]
                b1[j, i] = 1 / b1[i, j]

        b2 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b2[i, j] = normal[j, 1] / normal[i, 1]
                b2[j, i] = 1 / b2[i, j]

        b3 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b3[i, j] = normal[j, 2] / normal[i, 2]
                b3[j, i] = 1 / b3[i, j]

        b4 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b4[i, j] = normal[i, 3] / normal[j, 3]
                b4[j, i] = 1 / b4[i, j]

        b5 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b5[i, j] = normal[i, 4] / normal[j, 4]
                b5[j, i] = 1 / b5[i, j]

        b6 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b6[i, j] = normal[i, 5] / normal[j, 5]
                b6[j, i] = 1 / b6[i, j]

        b = [b1, b2, b3, b4, b5, b6]
        score1 = AHP(criteria, b).run()
        maxScore1 = score1.max()
        score1 = score1 / maxScore1 * 100  # 正常设备分数
        normalId = np.squeeze(normalId)
        id1 = dataframe.loc[list(normalId), "GIS-ID"]
        name1 = dataframe.loc[list(normalId), "GIS名"]
        rank1 = dataframe.loc[list(normalId), "标签"]
        # 写入到数据库中的结果
        # outputTime = time.strftime('%Y-%m-%d %H:%M:%S')

        outputTime1 = [outputTime for i in score1]
        score1 = score1.tolist()
        primary_key = [None for i in score1]
        toMysqlData1 = zip(primary_key, id1, name1, outputTime1, score1, rank1)
        toMysqlData1 = list(toMysqlData1)
    elif len(normal) == 1:
        # normalId = np.squeeze(normalId)
        id1 = dataframe.loc[normalId[0][0], "GIS-ID"]
        name1 = dataframe.loc[normalId[0][0], "GIS名"]
        rank1 = dataframe.loc[normalId[0][0], "标签"]
        # outputTime = time.strftime('%Y-%m-%d %H:%M:%S')
        toMysqlData1 = [(None, id1, name1, outputTime, 100, rank1)]

    # 异常的变压器
    if len(abnormal) > 1:
        c1 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c1[i, j] = abnormal[i, 0] / abnormal[j, 0]
                c1[j, i] = 1 / c1[i, j]

        c2 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c2[i, j] = abnormal[i, 1] / abnormal[j, 1]
                c2[j, i] = 1 / c2[i, j]

        c3 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c3[i, j] = abnormal[i, 2] / abnormal[j, 2]
                c3[j, i] = 1 / c3[i, j]

        c4 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c4[i, j] = abnormal[j, 3] / abnormal[i, 3]
                c4[j, i] = 1 / c4[i, j]

        c5 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c5[i, j] = abnormal[j, 4] / abnormal[i, 4]
                c5[j, i] = 1 / c5[i, j]

        c6 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c6[i, j] = abnormal[j, 5] / abnormal[i, 5]
                c6[j, i] = 1 / c6[i, j]

        c = [c1, c2, c3, c4, c5, c6]
        score2 = AHP(criteria, c).run()
        maxScore2 = score2.max()
        score2 = score2 / maxScore2 * 100  # 异常设备分数
        abnormalId = np.squeeze(abnormalId)
        id2 = dataframe.loc[list(abnormalId), "GIS-ID"]
        name2 = dataframe.loc[list(abnormalId), "GIS名"]
        rank2 = dataframe.loc[list(abnormalId), "标签"]
        # outputTime = time.strftime('%Y-%m-%d %H:%M:%S')
        outputTime2 = [outputTime for i in score2]
        score2 = score2.tolist()
        primary_key = [None for i in score2]
        toMysqlData2 = zip(primary_key, id2, name2, outputTime2, score2, rank2)
        toMysqlData2 = list(toMysqlData2)

    elif len(abnormal) == 1:
        id2 = dataframe.loc[abnormalId[0][0], "GIS-ID"]
        name2 = dataframe.loc[abnormalId[0][0], "GIS名"]
        rank2 = dataframe.loc[abnormalId[0][0], "标签"]
        # outputTime = time.strftime('%Y-%m-%d %H:%M:%S')
        toMysqlData2 = [(None, id2, name2, outputTime, 100, rank2)]

    if len(normal) != 0 and len(abnormal) != 0:
        return toMysqlData1, toMysqlData2
    elif len(normal) != 0 and len(abnormal) == 0:
        return toMysqlData1, None
    elif len(normal) == 0 and len(abnormal) != 0:
        return None, toMysqlData2
    else:
        return None, None



