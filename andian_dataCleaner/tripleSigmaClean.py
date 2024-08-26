import numpy as np
import pandas as pd
from logger import LOGGER


class TripleSigmaCleaner:
    def __init__(self, dataOld: pd.DataFrame):

        LOGGER.info("Triple-sigma-cleaner init.")

        # 首先得到feature个数 对每个feature单独进行3-sigma检验
        columns = dataOld.columns
        numFeature = dataOld.shape[-1]

        # 定义四个数组来存储
        self.means, self.stds, self.floors, self.ceilings = [], [], [], []
        for idx in range(numFeature):
            featureData = dataOld[columns[idx]].values.tolist()
            featureData = np.asarray(featureData, dtype=float)

            # 计算此列数据的均值和方差
            mean = np.mean(featureData, axis=0)
            std = np.std(featureData, axis=0)

            floor = mean - 3 * std
            ceiling = mean + 3 * std

            self.means.append(mean)
            self.stds.append(std)
            self.floors.append(floor)
            self.ceilings.append(ceiling)

        self.means, self.stds, self.floors, self.ceilings = np.array(
            self.means), np.array(self.stds), np.array(self.floors), np.array(
                self.ceilings)

    def validationNew(self, dataNew: pd.DataFrame):
        dataNewCol = dataNew.columns

        if dataNew.shape[0] == 1:
            dataNewArr = dataNew.values[0]

            # 判断是否符合3-sigma条件
            # 修改不符合条件的数据
            for i, val in enumerate(dataNewArr):
                dataNewArr[i] = float(
                    np.where(
                        ((val < self.floors[i]) | (val > self.ceilings[i])),
                        self.means[i], val))

            # 返回修改后的数据
            dataNewModified = pd.DataFrame(dataNewArr).T
            dataNewModified.columns = dataNewCol
        else:
            dataNewArr = dataNew.values

            for idx, insert in enumerate(dataNewArr):

                # 判断是否符合3-sigma条件
                # 修改不符合条件的数据
                for i, val in enumerate(insert):
                    insert[i] = float(
                        np.where(((val < self.floors[i]) |
                                  (val > self.ceilings[i])), self.means[i],
                                 val))
                dataNewArr[idx] = insert

            # 返回修改后的数据
            dataNewModified = pd.DataFrame(dataNewArr)
            dataNewModified.columns = dataNewCol

        # 返回前再次检查是否有空值
        if dataNewModified.isnull().values.any():
            dataNewModified = dataNewModified.fillna(0)
            LOGGER.warning("missing value exists in dataNewModified.")
        return dataNewModified
