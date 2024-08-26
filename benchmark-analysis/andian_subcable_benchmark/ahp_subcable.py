import numpy as np
import pandas as pd
import time
from ahp import AHP
import writeDatabase


def subcable_benchmark(dataframe, criteria, outputTime):

    dataframe.columns = ["海缆ID", "海缆名", "监测点ID", "电压", "电流", "功率", "光纤温度", "缆芯温度", "扰动能量", "标签"]

    if sum(dataframe.isna().sum()) != 0:
        dataframe["标签"] = dataframe["标签"].fillna("异常")
        dataframe = dataframe.fillna(0)

    # 更改标签，无风险为1类，其余为0类
    dataframe["标签"].loc[dataframe["标签"] == "无风险"] = "I级"
    dataframe["标签"].loc[dataframe["标签"] == "一级风险"] = "II级"
    dataframe["标签"].loc[dataframe["标签"] == "二级风险"] = "III级"
    dataframe["标签"].loc[dataframe["标签"] == "三级风险"] = "IV级"

    data = dataframe[["电压", "电流", "功率", "光纤温度", "缆芯温度", "扰动能量", "标签"]]

    x = data.values
    normalId = np.where(x[:, -1] == "I级")
    abnormalId = np.where(x[:, -1] != "I级")
    x = x[:, :-1].astype(float)
    if np.any(x == 0):
        print("数据中存在0值")
        x[np.where(x == 0)] = 0.01
    # x = np.delete(x, np.where(np.isnan(x))[0], axis=0)

    normal = x[normalId]
    abnormal = x[abnormalId]

    m = normal.shape[0]
    n = abnormal.shape[0]

    # 对每个准则，方案优劣排序
    # 对于正常的海缆
    if len(normal) > 1:
        b1 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b1[i, j] = normal[i, 0] / normal[j, 0]
                b1[j, i] = 1 / b1[i, j]

        b2 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b2[i, j] = normal[i, 1] / normal[j, 1]
                b2[j, i] = 1 / b2[i, j]

        b3 = np.zeros((m, m))
        for i in range(m):
            for j in range(i, m):
                b3[i, j] = normal[i, 2] / normal[j, 2]
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

        id1 = dataframe.loc[list(normalId), "海缆ID"]
        name1 = dataframe.loc[list(normalId), "海缆名"]
        monitor_id1 = dataframe.loc[list(normalId), "监测点ID"]
        rank1 = dataframe.loc[list(normalId), "标签"]
        # 写入到数据库中的结果
        outputTime1 = [outputTime for i in score1]
        score1 = score1.tolist()
        primary_key = [None for i in score1]
        toMysqlData1 = zip(primary_key, id1, name1, monitor_id1, outputTime1, score1, rank1)
        toMysqlData1 = list(toMysqlData1)

    elif len(normal) == 1:
        id1 = dataframe.loc[normalId[0][0], "海缆ID"]
        name1 = dataframe.loc[normalId[0][0], "海缆名"]
        monitor_id1 = dataframe.loc[normalId[0][0], "监测点ID"]
        rank1 = dataframe.loc[normalId[0][0], "标签"]
        toMysqlData1 = [(None, id1, name1, monitor_id1, outputTime, 100, rank1)]

    # 异常的海缆
    if len(abnormal) > 1:
        c1 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c1[i, j] = abnormal[j, 0] / abnormal[i, 0]
                c1[j, i] = 1 / c1[i, j]

        c2 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c2[i, j] = abnormal[j, 1] / abnormal[i, 1]
                c2[j, i] = 1 / c2[i, j]

        c3 = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                c3[i, j] = abnormal[j, 2] / abnormal[i, 2]
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
        id2 = dataframe.loc[list(abnormalId), "海缆ID"]
        name2 = dataframe.loc[list(abnormalId), "海缆名"]
        monitor_id2 = dataframe.loc[list(abnormalId), "监测点ID"]
        rank2 = dataframe.loc[list(abnormalId), "标签"]

        outputTime2 = [outputTime for i in score2]
        score2 = score2.tolist()
        primary_key = [None for i in score2]
        toMysqlData2 = zip(primary_key, id2, name2, monitor_id2, outputTime2, score2, rank2)
        toMysqlData2 = list(toMysqlData2)

    elif len(abnormal) == 1:
        id2 = dataframe.loc[abnormalId[0][0], "海缆ID"]
        name2 = dataframe.loc[abnormalId[0][0], "海缆名"]
        monitor_id2 = dataframe.loc[abnormalId[0][0], "监测点ID"]
        rank2 = dataframe.loc[abnormalId[0][0], "标签"]
        toMysqlData2 = [(None, id2, name2, monitor_id2, outputTime, 100, rank2)]

    if len(normal) != 0 and len(abnormal) != 0:
        return toMysqlData1, toMysqlData2
    elif len(normal) != 0 and len(abnormal) == 0:
        return toMysqlData1, None
    elif len(normal) == 0 and len(abnormal) != 0:
        return None, toMysqlData2
    else:
        return None, None
    # plt.rcParams['font.sans-serif'] = ['SimHei']
    # plt.figure(1)
    #
    # norm = plt.Normalize(70, 100)
    # norm_values = norm(score1)
    # map_vir = cm.get_cmap(name='brg')
    # colors = map_vir(norm_values)

    # plt.bar(id1, score1, 0.4, color=colors)
    # labels = ["{:.2f}".format(i) for i in score1]
    # for _id, _a, label, in zip(id1, score1, labels):
    #     plt.text(_id, _a, label, ha='center', va='bottom', fontsize=13)
    # plt.xlabel('设备名')
    # plt.ylabel('分数')
    #
    # plt.figure(2)
    # norm = plt.Normalize(70, 100)
    # norm_values = norm(score2)
    # map_vir = cm.get_cmap(name='rainbow')
    # colors = map_vir(norm_values)
    #
    # plt.bar(id2, score2, 0.4, color=colors)
    # labels = ["{:.2f}".format(i) for i in score2]
    # for _id, _a, label, in zip(id2, score2, labels):
    #     plt.text(_id, _a, label, ha='center', va='bottom', fontsize=13)
    # plt.xlabel('设备名')
    # plt.ylabel('分数')
    #
    # plt.show()
    #
    # print("海缆设备分数：")
    # for i, j in zip(id1, range(len(id1))):
    #     print(i, "的分数为:", score1[j])
    # print('\n')
    # print("标杆设备为：")
    # maxId = np.argmax(score1)
    # print(id1.iloc[maxId])


