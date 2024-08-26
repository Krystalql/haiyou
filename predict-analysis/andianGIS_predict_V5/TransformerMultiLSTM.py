import torch
import torch.nn as nn
import numpy as np
import math

# 定义模型
class MyLstm(nn.Module):#python中定义类使用class关键字，class后面紧接类名,类包含属性和方法
    def __init__(self, input_size, output_size, hidden_layer_size, num_layers, predict_steps):#在定义实例变量、实例方法时的第一个参数必须是self
        super().__init__()
        #以训练句子为例子，假如每个词是100维的向量，每个句子含有24个单词，一次训练10个句子。那么batch_size = 10, seq = 24, input_size = 100。
        self.input_size = input_size
        #input = Variable(torch.randn(5,3,10)) # 5行矩阵 每个矩阵是3行10列；10列是根据LSTM输入规定10列，词向量维度是10
        self.output_size = output_size
        self.num_layers = num_layers
        self.predict_steps = predict_steps
        self.hidden_layer_size = hidden_layer_size
        self.lstm = nn.LSTM(input_size=self.input_size, hidden_size=self.hidden_layer_size, num_layers=self.num_layers)
        # nn.LSTM(10, 20, 2)  # (1)输入的特征维度10列 (2)隐状态的特征维度20列 (3)num_layers = 2层
        self.linear = nn.Linear(self.hidden_layer_size, self.output_size)
        self.hidden_cell = (torch.zeros(self.num_layers, 1, self.hidden_layer_size),
                            torch.zeros(self.num_layers, 1, self.hidden_layer_size))

    def forward(self, input_seq):
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out[-1].view(-1, self.hidden_layer_size))
        return predictions.view(-1)


class MultiLstm:
    def __init__(self, args):
        self.timesteps = args.inputTimeStep  # 输入步数
        self.predict_steps = args.outputTimeStep  # 输出步数
        self.frd = args.predictTimeStep  # 滑动窗口预测的步数  default=6,
        self.num_layers = args.multi_numLayers  # 网络层数
        self.input_size = args.variablesNumber  # 输入维度
        self.hidden_layer_size = args.multi_hidden_layer_size
        self.output_size = args.variablesNumber  # 维度
        self.normalVariable = args.normalVariable  #包含最大值最小值的列表 8+8
        self.variables = args.variables
        self.scoreOutput_size = 1
        self.minNormalValue = args.minNormalValue
        self.maxNormalValue = args.maxNormalValue
        self.scoreRange = args.scoreRange

    # 主预测函数
    def predict(self, equip_id,  x):  #x=MultiData为18行7列，每个变量信息有18个，其中读取历史信息12个 + 预测信息6个
        # 输入检查
        error = []
        for i in range(self.frd):   #self.frd = args.predictTimeStep  滑动窗口预测的步数  default=6,
            error.append(50) #？？？？
        if len(x) == self.timesteps+self.frd:
            #len(x) 行数
            #self.timesteps+self.frd= 12 + 6 = 18
            for ele in x:  # 对每个数进行检查
                for e in ele:
                    if e > self.maxNormalValue or e < self.minNormalValue:
                        print("分数预测数据格式异常，进行容错，异常值："+str(e))
                        return error
            # 处理数据，归一化
            x = self.scoreNormalization(x)
            # 确定参数加载路径 模型训练程序NewTrain:多变量预测模型路径为：PATH = './model' + '/'  + equip_id+ variable + '_checkpoint.pth'
            PATH = './model' + '/' + equip_id +'score' + '_checkpoint.pth'
            # 生成模型
            scoreModel = MyLstm(self.input_size, self.scoreOutput_size, self.hidden_layer_size, self.num_layers,
                                self.predict_steps)
            scoreModel.load_state_dict(torch.load(PATH, map_location=torch.device("cpu"))) #cpu计算
            scoreModel.eval()
            score = []
            with torch.no_grad(): #是一个上下文管理器，被该语句 wrap 起来的部分将不会track 梯度。
                for i in range(len(x) - self.timesteps):  #len(x) - self.timesteps = 18-12=6
                    scoreModel.hidden_cell = (torch.zeros(self.num_layers, 1, self.hidden_layer_size),
                                              torch.zeros(self.num_layers, 1, self.hidden_layer_size)) #初始化隐藏层数据
                    predicted = scoreModel(torch.FloatTensor(x[i:i + self.timesteps])).item()
                    tempScore = float(self.scoreDeNormalization(predicted))
                    if tempScore < self.scoreRange[0]:  # 对结果再次进行检查容错 args.scoreRange = [30, 110]
                        tempScore = self.scoreRange[0]
                    if tempScore > self.scoreRange[1]:
                        tempScore = self.scoreRange[1]-0.1
                    if math.isnan(tempScore):
                        tempScore=90
                    score.append(tempScore)

            return score #长度为6
        else:
            print("分数预测数据步数异常，进行容错")
            return error

    # 归一化函数  利用normalVariable归一化字典， 这里的归一化字典，最大值最小值是哪里来的？

    def scoreNormalization(self, data):
        data = np.array(data) #化为矩阵
        for i in range(self.input_size): #input_size = args.variablesNumber  # 输入维度=变量个数 =7
            temp = float(self.normalVariable[self.variables[i]][0]) - float(self.normalVariable[self.variables[i]][1])
            #temp=最大值-最小值
            data[:, i] -= float(self.normalVariable[self.variables[i]][1])   #数据 - 最小值
            data[:, i] /= temp # 数据 = （数据- 最小值）/temp
        return data

    # 反归一化函数
    def scoreDeNormalization(self, data):
        score = self.variables[-1]
        return np.array(data) * (float(self.normalVariable[score][0] )- float(self.normalVariable[score][1])) + \
               float(self.normalVariable[score][1])


