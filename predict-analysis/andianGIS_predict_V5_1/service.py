from dao import DAO
from TransfromerSingleLSTM import SingleLstm
from TransformerMultiLSTM import MultiLstm

# 导入打印错误信息模块
import traceback
import numpy as np

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
                resX[:, col] = (X[:, col] - self.min_[col]) / 0.000001
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
        step = self.variablesNumber + 1  #?? step为什么要等于变量数+1  +1评分数据
        for i in range(len(self.variables)):
            self.normalVariable[self.variables[i]] = (trainData[i], trainData[i + step])
            #variables = ['voltage', 'current', 'apparent_power', 'winding_temperature', 'ae_partial_discharge',
            #'rf_partial_discharge', 'core_grounding_current', 'score']  # 变量名
            #?? 如normalVariable[voltage]=(第1个数据，7第个数据) 第一个，第7个最小
            #     normalVariable[current]=(第2个数据，第8个数据)
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
    def run_GIS(self):
        myDao = DAO(self.args)
        try:

            GISInfo = myDao.queryGISInfo()  ##包含GIS设备的列表
            print(GISInfo)
            for tid, equipName in GISInfo:       #遍历GIS,tid，equipName分别对应数据库中的equip_id,equio_name
                print(tid)
                # --------------------获得归一化最值------------#  对每个monitor使用独立的模型，归一化也分开
                normData = myDao.queryNormData2(tid)  # 返回的是 当前monitor 7最大值+7最小值的列表
                self.getTrainDict(normData) #生成最大最小值字典

                # --------------------获得前n数据------------#
                GISData, ti = myDao.queryVariable(tid, self.inputTimeStep) ###从实际库中，返回一个时间倒序的矩阵每行为每个GIS的信息，和第一个时间点 inputTimeStep=12
                ##GISData为一个矩阵，有inputTimeStep行 ，6列，每行包含各个变量的信息。
                ##       变量1
                ## 时间1[  1, 2, 3, 4, 5, 6
                ##        1, 2, 3, 4, 5, 6
                ##        .................
                ##        1, 2, 3, 4, 5, 6
                ##        1, 2, 3, 4, 5, 6 ]
                if len(GISData) == 0 or len(GISData) is not self.inputTimeStep: #inputTimeStep 12
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
                    # --------------------单变量预测------------------#
                mySingleLstm = SingleLstm(self.args) #SingleLstm为TransfromerSingleLSTM文件中定义的一个类,調用時必须加（），先对其实例化
                                                     #self.args将SERVICE中的args作为参数给SingleLstm
                singlePredictResult = []  #定义预测结果列表
                # singleNormPredictResult = []
                for i in range(self.variablesNumber):
                    result = mySingleLstm.predict(tid,GISData[:, i], self.variables[i])

                    singlePredictResult.append(result) #将预测结果写到预测表里
                #for 循环结束后，singlePredictResult为一个 二维数组  [[1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6]]
                #[1, 2, 3, 4, 5, 6]为某个变量的预测值（6步）
                singlePredictResult = np.array(singlePredictResult).transpose(1, 0)


                # --------------------多变量预测------------------#
                MultiData = np.concatenate([GISData, singlePredictResult], axis=0)#np.concatenate数组拼接函数 axis=0 按照行拼接。
                ##拼接后 MultiData为18行6列，每个变量信息有18个，其中读取历史信息12个 + 预测信息6个
                myMultiLstm = MultiLstm(self.args)  #调用 TransformerMultiLSTM 中的 MultiLstm 类
                score = myMultiLstm.predict( tid,MultiData )
                # print("score:",score)
                rank = self.getRank(score)
                score = np.array(score).reshape(-1, 1) #.reshape(-1, 1)变成一列
                rank = np.array(rank).reshape(-1, 1)   #.reshape(-1, 1)变成一列

                norm = MinMaxScaler()
                # 进行数据归一化
                # ["pd_peak", "discharge_amplitude", "discharge_times", "gis_pressure", "micro_water_content", "gis_temperature"]
                normdata = singlePredictResult
                normdata = normdata.astype('float32')
                norm.fit(normdata)
                normdata = norm.transform(normdata)

                singlePredictData = np.concatenate([singlePredictResult,normdata,score, rank], axis=1)#np.concatenate数组拼接函数 axis=1 按照列拼接。
                # singlePredictData = np.concatenate(
                #     [singlePredictResult, singleNormPredictResult, score, rank, singleWarnPredictResult], axis=1)

                #-----------------插入告警值----------------------#
                # myDao.insertWarnResult(tid, warnData, equipName)
                # 插入数据库

                # 先删除前n-1条单变量和多变量的记录
                myDao.deleteSingleVariable(tid,  (self.predictTimeStep - 1) * 1)
                print("已在andian_gis_predict中删除数据")

                # 单变量插入历史表
                # print(tid+"的数据开始写入数据库")
                myDao.insertVariable(tid, equipName,  singlePredictData, ti) #ti为读取的第一个时间点（最早的）
                print("已在andian_gis_predict中写入数据")

                # --------------------生成模型参数------------------#

                myDao.insertModelParam(tid, equipName, self.dbModelsNumber[0],
                                       generateModelParam(self.args.dbModelsNumber[0]))


        except Exception as e:
                print("程序出现异常")
                traceback.print_exc()
                #使用traceback函数打印错误，错误行，错误位置及错误明细。
        finally:
                myDao.closeConnection()
