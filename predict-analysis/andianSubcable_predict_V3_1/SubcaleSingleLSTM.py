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
        self.lstm = nn.LSTM(input_size=self.input_size,
                            hidden_size=self.hidden_layer_size,
                            num_layers=self.num_layers)
        self.linear = nn.Linear(self.hidden_layer_size, self.output_size)

        self.hidden_cell = (torch.zeros(self.num_layers, 1, self.hidden_layer_size),
                            torch.zeros(self.num_layers, 1, self.hidden_layer_size))

    def forward(self, input_seq):
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out.view(-1, self.hidden_layer_size))
        return predictions.view(len(input_seq))[-self.predict_steps:]

class SingleLstm:
    def __init__(self, args):
        self.timesteps = args.inputTimeStep  # 输入步数
        self.predict_steps = args.outputTimeStep  # 输出步数
        self.frd = args.predictTimeStep  # 滑动窗口预测的步数
        self.num_layers = args.single_numLayers  # 网络层数
        self.input_size = 1
        self.hidden_layer_size = args.single_hidden_layer_size
        self.output_size = 1
        self.normalVariable = args.normalVariable
        self.minNormalValue = args.minNormalValue
        self.maxNormalValue = args.maxNormalValue

    # 主预测函数
    def predict(self, tid, x, variable):
        # 输入检查
        error = []
        patch = x[0]
        for i in range(self.frd):
            error.append(patch)
        error = np.array(error)
        if len(x) == self.timesteps:
            for e in x:  # 对每个数进行检查
                if e > self.maxNormalValue or e < self.minNormalValue:
                    print("单变量" + variable + "数据格式异常，进行容错，异常值：" + str(e))
                    return error
            # 处理数据，归一化
            x = self.Normalization(x, variable)
            # 转化为Tenser
            # x = torch.FloatTensor(x)
            # 确定参数加载路径
            PATH = './modelParameter' + '/' + tid + '_' + variable + '_checkpoint.pth'
            # 生成模型
            model = MyLstm(self.input_size, self.output_size,
                           self.hidden_layer_size, self.num_layers,
                           self.predict_steps)
            model.load_state_dict(
                torch.load(PATH, map_location=torch.device('cpu')), )
            model.eval()
            predict_y = []  # 预测y列表
            test_inputs = x.tolist()  # 滑动窗口预测列表
            while len(predict_y) < self.frd:
                seq = torch.FloatTensor(test_inputs[-self.timesteps:])
                with torch.no_grad():
                    model.hidden = (torch.zeros(self.num_layers, 1,
                                                self.hidden_layer_size),
                                    torch.zeros(self.num_layers, 1,
                                                self.hidden_layer_size))
                    out = model(seq).item()
                test_inputs.append(out)
                predict_y.append(out)
            actual_predictions = self.DeNormalization(predict_y, variable)
            # 检查预测的数据和容错
            actual_predictions[
                actual_predictions > self.maxNormalValue] = self.maxNormalValue
            actual_predictions[
                actual_predictions < self.minNormalValue] = self.minNormalValue
            return actual_predictions
        else:
            print("单变量" + variable + "步数异常，进行容错")
            return error

    # 归一化函数
    def Normalization(self, data, varId):
        if self.normalVariable[varId][0] - self.normalVariable[varId][1] == 0.0:
            temp = 0.01
        else:
            temp = self.normalVariable[varId][0] - self.normalVariable[varId][1]
        return (np.array(data) - self.normalVariable[varId][1]) / temp

    # 反归一化函数
    def DeNormalization(self, data, varId):
        return np.array(data) * (self.normalVariable[varId][0] - self.normalVariable[varId][1]) + \
               self.normalVariable[varId][1]


if __name__ == '__main__':
    print()
