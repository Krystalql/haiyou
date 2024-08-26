from dao import DAO
from TransfromerSingleLSTM import SingleLstm
from TransformerMultiLSTM import MultiLstm

# 导入打印错误信息模块
import traceback
import numpy as np

# 生产模型随机参数 ??为什么是随机产生参数？？
def generateModelParam(num):
    return np.random.random(num).tolist()
# generateModelParam(self.args.dbModelsNumber[i])

class  MinMaxScaler:
    def __init__(self):
        self.min_ = None
        self.max_ = None
    def fit(self,X):
        '''根据训练数据集X获得数据的最小值和最大值'''
        self.min_ = np.array([np.min(X[:,i]) for i in range(X.shape[1])])
        self.max_ = np.array([np.max(X[:,i]) for i in range(X.shape[1])])
        return self
    def transform(self,X):
        '''将X根据MinMaxScaler进行最值归一化处理'''
        resX = np.empty(shape=X.shape,dtype=float)
        for col in range(X.shape[1]):
            if (self.max_[col]-self.min_[col]) == 0.0:
                resX[:, col] = 0.5
            else:
                resX[:,col] = (X[:,col]-self.min_[col]) / (self.max_[col]-self.min_[col])
        return resX


class SERVICE: #定于一个类
    def __init__(self, args):#使用 def 关键字来定义一个方法，与一般函数定义不同，类方法必须包含参数 self, 且为第一个参数，self 代表的是类的实例。
        self.args = args
        self.variablesNumber = args.variablesNumber  # 变量个数 =7
        self.predictTimeStep = args.predictTimeStep  # 预测步数
        self.inputTimeStep = args.inputTimeStep  # 输入模型步数
        self.variables = args.variables  # 变量名列表
        self.normalVariable = {}  # 归一化值列表 8+8
        self.dbModelsNumber = args.dbModelsNumber
        self.rank = args.rank
        self.scoreRange = args.scoreRange #args.scoreRange = [30, 110]

    # 生成归一化上下限 字典
    def getTrainDict(self, trainData):
        step = self.variablesNumber + 1
        for i in range(len(self.variables)):
            self.normalVariable[self.variables[i]] = (trainData[i], trainData[i + step])

        self.args.normalVariable = self.normalVariable
        ##normalVariable为一个字典：  { voltage_max: (1, 2), current_max: (2, 3).........score_Min: (1,2 ) }

    # 将分数转化成等级
    def getRank(self, score):
        rank = []
        compute = (self.scoreRange[1] - self.scoreRange[0]) / len(self.rank)
        for x in score:
            rank.append(self.rank[int((x-self.scoreRange[0]) // compute)])
        return rank

    # 主运行函数
    def run_transformer(self):
        myDao = DAO()
        try:#在程序开发中，如果对某些代码的执行不能确定(程序语法完全正确),可以增加try来捕获异常.except:出现错误的处理

            #旧版代码
            # --------------------获得归一化最值------------#  新版要对每个变压器使用独立的模型，归一化也要分开搞
            #normData = myDao.queryNormData() #返回的是8最大值+8最小值的列表
            #self.getTrainDict(normData)
            # ----------------获得每个变压器最大值-----------#
            # maxData = myDao.queryMaxData()

            # --------------------获得变压器信息------------#
            transformInfo = myDao.queryTransformerInfo()  ##一个包含变压器信息的列表
            for tid, equipName in transformInfo:       #遍历几个不同的变压器,tid，equipName分别对应数据库中的equip_id,equio_name(前两列)
                print(tid)
                # --------------------获得前n数据------------#
                transformerData, ti = myDao.queryVariable(tid, self.inputTimeStep) ###从实际库中，返回一个时间倒序的矩阵每行为每个变压器的信息，和第一个时间点 inputTimeStep=12
                ##transformerData为一个矩阵，有12行 ，17列，每行包含各个变量的信息。
                ##       变量1
                ## 时间1[  1, 2, 3, 4, 5, 6, 7
                ##        1, 2, 3, 4, 5, 6, 7
                ##        .................
                ##        1, 2, 3, 4, 5, 6, 7
                ##       1, 2, 3, 4, 5, 6, 7  ]
                if len(transformerData) == 0 or len(transformerData) is not self.inputTimeStep:
                    continue     ##变压器信息数量等于 = 输入时间步数
                predictTi = myDao.queryTime(tid, 1)   ##queryTime返回预测列表最新值的前180分钟
                #if predictTi == ti:
                                            #若最新预测的时间往前推3个小时，与实际数据的最新时间一样，则说明，实际数据没有更新！#为什么要推三个小时？因为每次预测六步，每步半个小时
                                            # continue #如果时间没变，说明数据没更新，就不预测了
                                            # --------------------获得告警值----------------#
                                            # warnData = myDao.queryWarnData(tid)
                                            # warnData[0] /= 100
                                            # if tid not in maxData.keys():
                                            #     maxData[tid] = np.ones(self.variablesNumber)
                                            # else:
                                            #     maxData[tid][maxData[tid] == 0.] = 1.0
                # --------------------获得归一化最值------------#  对每个变压器使用独立的模型，归一化分开
                normData = myDao.queryNormData2(tid)  # 返回的是 当前变压器 17最大值+17最小值的列表
                self.getTrainDict(normData)  # 生成最大最小值字典
                    # --------------------单变量预测------------------#
                mySingleLstm = SingleLstm(self.args) #SingleLstm为TransfromerSingleLSTM文件中定义的一个类,調用時必须加（），先对其实例化
                                                     #self.args将SERVICE中的args作为参数给SingleLstm
                singlePredictResult = []  #定义预测结果列表
                # singleNormPredictResult = []
                for i in range(self.variablesNumber):
                    result = mySingleLstm.predict(tid, transformerData[:, i], self.variables[i])
                                 #result长度为6
                                 #mySingleLstm.predict调用了TransfromerSingleLSTM文件中的SingleLstm类的predict函数
                                 #输入是数据与变量名    [:, i]取第i列
                    # if len(maxData) is 0:
                    #     a = [0.5] * self.predictTimeStep
                    #     normResult = np.array(a)
                    # elif tid not in maxData.keys():
                    #     a = [0.5] * self.predictTimeStep
                    #     normResult = np.array(a)
                    # else:
                    #     normResult = result / maxData[tid][i]
                    #     normResult[normResult > 1.0] = 1.0
                    # singleNormPredictResult.append(normResult)
                    singlePredictResult.append(result) #将预测结果写到预测表里
                #for 循环结束后，singlePredictResult为一个 二维数组  [[1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6]]
                #[1, 2, 3, 4, 5, 6]为某个变量的预测值（6步）
                singlePredictResult = np.array(singlePredictResult).transpose(1, 0)
                #singlePredictResult为：
                #      变量1
                #步数1[  1   1 .... 1          16列 七个变量
                #       2   2 .....2          每列为某个变量的预测值，6行为6步
                #       3   3 .....3
                #       4   4 .....4
                #       5   5 .....5
                #       6   6 .....6  ]
                #np.array(singlePredictResult)先将二维数组转化为矩阵
                #transport（1，0）表示行与列调换了位置；？为什么：方便后面与transformerData拼接

                # singleNormPredictResult = np.array(singleNormPredictResult).transpose(1, 0)
                # if len(warnData) is 0:
                #     singleWarnPredictResult = np.zeros(singlePredictResult.shape, dtype="float")
                #     singleWarnPredictResult.fill(0.5)
                #     warnData = np.array([100, 35, 630, 20, 20, 200, 100])
                # else:
                #     singleWarnPredictResult = warnData / maxData[tid]
                #     singleWarnPredictResult[singleWarnPredictResult > 1.0] = 1.0
                #     singleWarnPredictResult = singleWarnPredictResult.reshape(-1, self.variablesNumber)
                #     singleWarnPredictResult = np.repeat(singleWarnPredictResult, self.predictTimeStep, axis=0)


                # --------------------多变量预测------------------#
                MultiData = np.concatenate([transformerData, singlePredictResult], axis=0)#np.concatenate数组拼接函数 axis=0 按照行拼接。
                # print('MultiData.shape:',MultiData.shape)
                ##拼接后 MultiData为18行16列，每个变量信息有18个，其中读取历史信息12个 + 预测信息6个
                myMultiLstm = MultiLstm(self.args)  #调用 TransformerMultiLSTM 中的 MultiLstm 类
                score = myMultiLstm.predict( tid , MultiData )
                # print("score",score)
                rank = self.getRank(score)
                score = np.array(score).reshape(-1, 1) #.reshape(-1, 1)变成一列
                rank = np.array(rank).reshape(-1, 1)   #.reshape(-1, 1)变成一列
                # print(singlePredictData)
                # print("singlePredictData.shape:",singlePredictData.shape)
                # singlePredictData = np.concatenate(
                #     [singlePredictResult, singleNormPredictResult, score, rank, singleWarnPredictResult], axis=1)

                norm=MinMaxScaler()
                # 进行数据归一化
                normdata1=singlePredictResult
                normdata1 = normdata1.astype('float32')
                norm.fit(normdata1)
                normdata1=norm.transform(normdata1)
                insertdata=np.concatenate([singlePredictResult, score, rank,normdata1], axis=1)

                #-----------------插入告警值----------------------#
                # myDao.insertWarnResult(tid, warnData, equipName)
                # 插入数据库

                # 先删除前n-1条单变量和多变量的记录
                myDao.deleteSingleVariable(tid, (self.predictTimeStep - 1) * 1)
                print("已在andian_transformer_predict中删除数据")

                # 单变量插入历史表
                # print(tid+"的数据开始写入数据库")
                myDao.insertVariable(tid, equipName, insertdata, ti) #ti为读取的第一个时间点（最早的）
                print("已在andian_transformer_predict中写入数据")

                # --------------------生成模型参数------------------#
                for i in range(len(self.dbModelsNumber)):#dbModelsNumber = [18, 6] , len(self.dbModelsNumber)=2
                    myDao.insertModelParam(tid, equipName, i + 1, self.dbModelsNumber[i],
                                           generateModelParam(self.args.dbModelsNumber[i]))

                print("预测结束")

        except Exception as e:
                print("程序出现异常")
                traceback.print_exc()
                #使用traceback函数打印错误，错误行，错误位置及错误明细。
        finally:
                myDao.closeConnection()
