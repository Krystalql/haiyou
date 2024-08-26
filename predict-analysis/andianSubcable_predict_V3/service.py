from dao import DAO
from SubcaleSingleLSTM import SingleLstm
from SubcableMultiLSTM import MultiLstm

# 导入打印错误信息模块
import traceback
import numpy as np

# 生产模型随机参数
def generateModelParam(num):
    return np.random.random(num).tolist()


class SERVICE:
    def __init__(self, args):
        self.args = args
        self.variablesNumber = args.variablesNumber  # 变量个数
        self.predictTimeStep = args.predictTimeStep  # 预测步数
        self.inputTimeStep = args.inputTimeStep  # 输入模型步数
        self.variables = args.variables  # 变量名列表
        self.normalVariable = {}  # 归一化值列表
        self.dbModelsNumber = args.dbModelsNumber
        self.rank = args.rank
        self.scoreRange = args.scoreRange
        self.warn = args.warn

    def getTrainDict(self, trainData):
        step = self.variablesNumber + 1
        for i in range(step):
            self.normalVariable[self.variables[i]] = (trainData[i], trainData[i + step])
        self.args.normalVariable = self.normalVariable

    # 将分数转化成等级
    def getRank(self, score):
        rank = []
        compute = (self.scoreRange[1] - self.scoreRange[0]) / len(self.rank)
        for x in score:
            rank.append(self.rank[int((x-self.scoreRange[0]) // compute)])
        return rank

    def run_transformer(self):
        myDao = DAO(self.args)
        try:
            normData = myDao.queryNormData()  # 获得归一化最值数据
            self.getTrainDict(normData)
            transformInfo = myDao.querySubCableInfo()  # 获得海缆id信息
            for tid, equipName in transformInfo:  # 对所有海缆进行遍历
                warnData = myDao.queryWarnData(tid)  # 获得海缆的告警值
                maxData = myDao.queryMaxData(tid)  # 获得海缆的最大值
                subCableKeySector = myDao.querySubCableKeySector(tid)  # 获得区段id和关键点id信息
                if len(subCableKeySector) == 0:
                    continue
                for section_id, monitorList in subCableKeySector.items():  # 对一条海缆所有区段进行遍历
                    for monitor_id in monitorList:  # 对一个区段关键点进行遍历
                        subCableData, ti = myDao.queryVariable(tid, monitor_id, self.inputTimeStep)
                        if len(subCableData) == 0 or len(subCableData) != self.inputTimeStep:
                            # print(len(subCableData))
                            continue
                        predictTi = myDao.queryTime(tid, monitor_id, 1)
                        # if predictTi == ti:
                        #     continue
                        # --------------------单变量预测------------------#
                        mySingleLstm = SingleLstm(self.args)
                        singlePredictResult = []
                        singleNormPredictResult = []
                        if monitor_id not in maxData.keys():
                            maxData[monitor_id] = np.ones(self.variablesNumber)
                        else:
                            maxData[monitor_id][maxData[monitor_id] == 0.] = 1.0
                        for i in range(self.variablesNumber):
                            result = mySingleLstm.predict(tid,subCableData[:, i], self.variables[i])
                            # print("result:", result)
                            if len(maxData) is 0:  # 为这个nt系统容错
                                a = [0.5] * self.predictTimeStep  # 为这个nt系统容错
                                normResult = np.array(a)  # 为这个nt系统容错
                            elif monitor_id not in maxData.keys():  # 为这个nt系统容错
                                a = [0.5] * self.predictTimeStep  # 为这个nt系统容错
                                normResult = np.array(a)  # 为这个nt系统容错
                            else:
                                normResult = result / maxData[monitor_id][i]
                                normResult[normResult > 1.0] = 1.0
                            singleNormPredictResult.append(normResult)
                            singlePredictResult.append(result)
                        singlePredictResult = np.array(singlePredictResult).transpose(1, 0)
                        singleNormPredictResult = np.array(singleNormPredictResult).transpose(1, 0)
                        if len(warnData) is 0 or section_id not in warnData.keys():
                            sinWarnPreResult = np.zeros(singlePredictResult.shape, dtype="float")
                            sinWarnPreResult.fill(0.5)
                            warnData[section_id] = np.array(self.warn)
                        else:
                            sinWarnPreResult = warnData[section_id] / maxData[monitor_id]
                            sinWarnPreResult[sinWarnPreResult > 1.0] = 1.0
                            sinWarnPreResult = sinWarnPreResult.reshape(-1, self.variablesNumber)
                            sinWarnPreResult = np.repeat(sinWarnPreResult, self.predictTimeStep, axis=0)
                        # --------------------多变量预测------------------#
                        MultiData = np.concatenate([subCableData, singlePredictResult], axis=0)
                        myMultiLstm = MultiLstm(self.args)
                        score = myMultiLstm.predict(tid,MultiData)
                        rank = self.getRank(score)
                        score = np.array(score).reshape(-1, 1)
                        rank = np.array(rank).reshape(-1, 1)
                        singlePredictData = np.concatenate(
                            [singlePredictResult, singleNormPredictResult, score, rank, sinWarnPreResult], axis=1)
                        # singlePredictData = np.concatenate(
                        #     [singlePredictResult, score, rank], axis=1)
                        # 告警值插入数据库
                        myDao.insertWarnResult(tid, monitor_id, warnData[section_id], equipName)
                        # 插入数据库
                        # 先删除前n-1条单变量和多变量的记录
                        # myDao.deleteSingleVariable(tid, monitor_id, (self.predictTimeStep - 1) * 1)
                        # 单变量插入历史表
                        myDao.insertVariable(tid, equipName, monitor_id, singlePredictData, ti)
                        # --------------------生成模型参数------------------#
                        for i in range(len(self.dbModelsNumber)):
                            myDao.insertModelParam(tid, equipName, monitor_id, i + 1, self.dbModelsNumber[i],
                                                    generateModelParam(self.args.dbModelsNumber[i]))

        except Exception as e:
            print("程序出现异常")
            traceback.print_exc()
        finally:
            myDao.closeConnection()
