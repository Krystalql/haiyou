import torch
import torch.nn as nn
import numpy as np


# 定义模型 pytorch中的 nn模块可以方便的构建网络
class MyLstm(nn.Module): # 继承了nn.Module并且和super().__init__() 使用。
    # 在定义自已的网络的时候，需要继承nn.Module类，并重新实现构造函数__init__构造函数和forward这两个方法。
    # nn.Module是nn中十分重要的类,包含网络各层的定义及forward方法。
    def __init__(self, input_size, output_size, hidden_layer_size, num_layers, predict_steps): # __init__()方法（函数）又被称为构造器（constructor）或构造函数，第一个参数必须为self.
        #两周构造方式，这里为其中一种：def __init__(self,a,b,c.....)
        #                           self.a=a
        #                           self.b=b
        #                           self.c=c
        #                           ........
        super().__init__() #继承父类的init方法，同样可以使用super()去继承其他方法。
        self.input_size = input_size
        self.output_size = output_size
        self.num_layers = num_layers
        self.predict_steps = predict_steps
        self.hidden_layer_size = hidden_layer_size
        self.lstm = nn.LSTM(input_size=self.input_size,
                            hidden_size=self.hidden_layer_size,
                            num_layers=self.num_layers)
        #实例化了一个LSTM的网络
        self.linear = nn.Linear(self.hidden_layer_size, self.output_size)
        #设置全连接层
        self.hidden_cell = (torch.zeros(self.num_layers, 1, self.hidden_layer_size),
                            torch.zeros(self.num_layers, 1, self.hidden_layer_size))
        #确定h0和c0，分别是hidden和cell的初始状态，然后，使用output, (hn, cn) = lstm(x, (h0, c0))得到输出。


    def forward(self, input_seq):
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out.view(-1, self.hidden_layer_size))
        #将LSTM与全连接层连接起来
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

    # 主预测函数 滑动窗口预测
    def predict(self, tid, x, variable):
        # 输入检查
        error = []      #定义异常数据列表
        patch = x[0]    #定义输入列表
        for i in range(self.frd): #frd=predictTimeStep 预测未来总步数
            error.append(patch)   #将patch填充到error中 ？？？为什么？？？
        error = np.array(error)   #np.array将数据转化为矩阵
        if len(x) == self.timesteps: # timesteps = inputTimeStep输入步数= 12
            for e in x:  # 对每个数进行检查
                if e > self.maxNormalValue or e < self.minNormalValue:   #maxNormalValue :default=9999.0  minNormalValue: default=0
                    print("单变量"+variable+"数据格式异常，进行容错，异常值：" + str(e)) #str() 函数将对象转化为适于人阅读的形式
                    return error
            # 处理数据，归一化
            x = self.Normalization(x, variable)
            # 转化为Tenser
            # x = torch.FloatTensor(x)
            # 确定参数加载路径
            PATH = './model' + '/' + tid + variable + '_checkpoint.pth'
            # 生成模型
            model = MyLstm(self.input_size, self.output_size, self.hidden_layer_size, self.num_layers,
                           self.predict_steps) #MyLstm的五个参数。
            model.load_state_dict(torch.load(PATH, map_location=torch.device("cpu")))
            predict_y = []  # 预测y列表
            test_inputs = x.tolist()  # 滑动窗口预测列表 .tolist()将数组或者矩阵转换成列表
            while len(predict_y) < self.frd: #predict_y存放了预测值，若预测值个数小于6就继续预测，即为每次预测6步。
                seq = torch.FloatTensor(test_inputs[-self.timesteps:])#[-self.timesteps:]取最后12位数据作为输入，torch.FloatTensor将list、numpy转化为tensor.
                with torch.no_grad(): #torch.no_grad() 是一个上下文管理器，被该语句 wrap 起来的部分将不会track 梯度。不会进行反向传播的求导
                    model.hidden = (torch.zeros(self.num_layers, 1, self.hidden_layer_size),
                                    torch.zeros(self.num_layers, 1, self.hidden_layer_size))
                    out = model(seq).item() #计算输出
                test_inputs.append(out) #将新的预测值加入的输入中。
                predict_y.append(out)
            actual_predictions = self.DeNormalization(predict_y, variable)  #self.调用类中的函数前要加self.
            # 检查预测的数据和容错
            actual_predictions[actual_predictions > self.maxNormalValue] = self.maxNormalValue
            actual_predictions[actual_predictions < self.minNormalValue] = self.minNormalValue
            return actual_predictions #长度为6的数组
        else:
            print("单变量" + variable + "步数异常，进行容错")
            return error

    # 输入归一化函数
    def Normalization(self, data, varId):
        if float(self.normalVariable[varId][0]) - float(self.normalVariable[varId][1]) == 0.0:
            temp = 0.01
        else:
            temp = float(self.normalVariable[varId][0]) - float(self.normalVariable[varId][1])

        return (np.array(data) - float(self.normalVariable[varId][1])) / temp
        #?? normalVariable[varId][1])由service.py中的getTrainDict

    # 反归一化函数
    def DeNormalization(self, data, varId):
        return np.array(data) * (float(self.normalVariable[varId][0]) - float(self.normalVariable[varId][1])) + \
               float(self.normalVariable[varId][1])


if __name__ == '__main__':
    #利用“name”属性即可控制Python程序的运行方式。例如，编写一个包含大量可被其他程序利用的函数的模块，而不希望该模块可以直接运行
    print()

