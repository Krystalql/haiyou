import torch
import torch.nn as nn
import numpy as np


# 定义模型
class MyLstm(nn.Module):
    def __init__(self, input_size, output_size, hidden_layer_size, num_layers, predict_steps):
        super().__init__()
        self.input_size = input_size
        self.output_size = output_size
        self.num_layers = num_layers
        self.predict_steps = predict_steps
        self.hidden_layer_size = hidden_layer_size
        self.lstm = nn.LSTM(input_size=self.input_size, hidden_size=self.hidden_layer_size, num_layers=self.num_layers)
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
        self.frd = args.predictTimeStep  # 滑动窗口预测的步数
        self.num_layers = args.multi_numLayers  # 网络层数
        self.input_size = args.variablesNumber  # 输入维度
        self.hidden_layer_size = args.multi_hidden_layer_size
        self.output_size = args.variablesNumber  # 维度
        self.normalVariable = args.normalVariable
        self.variables = args.variables
        self.scoreOutput_size = 1
        self.minNormalValue = args.minNormalValue
        self.maxNormalValue = args.maxNormalValue
        self.scoreRange = args.scoreRange

    # 主预测函数
    def predict(self, tid, x):
        # 输入检查
        error = []
        for i in range(self.frd):
            error.append(50)
        if len(x) == self.timesteps + self.frd:
            for ele in x:  # 对每个数进行检查
                for e in ele:
                    if e > self.maxNormalValue or e < self.minNormalValue:
                        print("分数预测数据格式异常，进行容错，异常值：" + str(e))
                        return error
            # 处理数据，归一化
            x = self.scoreNormalization(x)
            # 确定参数加载路径
            PATH = './modelParameter' + '/' + 'subCable_score_' + tid + '_checkpoint.pth'
            # 生成模型
            scoreModel = MyLstm(self.input_size, self.scoreOutput_size,
                                self.hidden_layer_size, self.num_layers,
                                self.predict_steps)
            scoreModel.load_state_dict(
                torch.load(PATH, map_location=torch.device('cpu')))
            scoreModel.eval()
            score = []
            with torch.no_grad():
                for i in range(len(x) - self.timesteps):
                    scoreModel.hidden_cell = (torch.zeros(
                        self.num_layers, 1, self.hidden_layer_size),
                                              torch.zeros(
                                                  self.num_layers, 1,
                                                  self.hidden_layer_size))
                    predicted = scoreModel(
                        torch.FloatTensor(x[i:i + self.timesteps])).item()
                    tempScore = float(self.scoreDeNormalization(predicted))
                    if tempScore < self.scoreRange[0]:  # 对结果再次进行检查容错
                        tempScore = self.scoreRange[0]
                    if tempScore > self.scoreRange[1]:
                        tempScore = self.scoreRange[1] - 0.1
                    score.append(tempScore)
            return score
        else:
            print("分数预测数据步数异常，进行容错")
            return error

    # 归一化函数
    def scoreNormalization(self, data):
        data = np.array(data)
        for i in range(self.input_size):
            if self.normalVariable[self.variables[i]][0] - self.normalVariable[
                    self.variables[i]][1] == 0.0:
                temp = 0.001
            else:
                temp = self.normalVariable[self.variables[i]][
                    0] - self.normalVariable[self.variables[i]][1]
            data[:, i] -= self.normalVariable[self.variables[i]][1]
            data[:, i] /= temp
        return data

    # 反归一化函数
    def scoreDeNormalization(self, data):
        score = self.variables[-1]
        return np.array(data) * (self.normalVariable[score][0] - self.normalVariable[score][1]) + \
               self.normalVariable[score][1]
