import time  # 时间模块
import os   #   对文件，文件夹执行操作的一个模块。
import argparse
from service import SERVICE #从service文件中导入SERVICE类
#argparse 模块是 Python 内置的一个用于命令项选项与参数解析的模块
#三个步骤：
#1、使用 argparse 的第一步是创建一个 ArgumentParser ：parser = argparse.ArgumentParser(description='LSTM-transformer predict')
#2、添加参数——调用 add_argument() 方法添加参数
#3、解析参数——使用 parse_args() 解析添加的参数
parser = argparse.ArgumentParser(description='LSTM-transformer predict')#对 ArgumentParser 构造方法的调用都会使用 description= 关键字参数


# 添加参数，定义程序需要的参数以及其默认值
'''名字，type参数类型，default默认值，help:参数描述，metavar'''
parser.add_argument('--interval', type=int, required=False, default=1800,
                    help='Run interval')  # 脚本运行间隔,0表示只运行一次，其他数值表示间隔秒数
parser.add_argument('--variablesNumber', type=int, required=False, default=14, help='variablesNumber')  # 预测变量个数7+9气体  #required表示参数是否可以省略
parser.add_argument('--predictTimeStep', type=int, required=False, default=6, help='predictTimeStep')  # 预测未来总步数
parser.add_argument('--inputTimeStep', type=int, required=False, default=12, help='inputTimeStep')  # 一次输入步数
parser.add_argument('--outputTimeStep', type=int, required=False, default=1, help='outputTimeStep')  # 一次输出步数
parser.add_argument('--single_numLayers', type=int, required=False, default=1, help='single_numLayers')  # 单变量层数，2
parser.add_argument('--single_hidden_layer_size', type=int, required=False, default=64,
                    help='single_hidden_layer_size')  # 单变量隐藏层节点个数，128
parser.add_argument('--multi_numLayers', type=int, required=False, default=1, help='multi_numLayers')  # 多变量层数，2
parser.add_argument('--multi_hidden_layer_size', type=int, required=False, default=64,
                    help='multi_hidden_layer_size')  # 多变量隐藏层节点个数，128
parser.add_argument('--maxNormalValue', type=float, required=False, default=9999.0,
                    help='maxAbnormalValue')  # 最大正常值范围，超过该值系统进行容错
parser.add_argument('--minNormalValue', type=float, required=False, default=0.0,
                    help='maxAbnormalValue')  # 最小正常值范围，超过该值系统进行容错
#通过parser.add_argument定义后的参数，被加入到args下。如 args.minNormalValue

args = parser.parse_args()#ArgumentParser 通过 parse_args() 方法解析参数。它将检查命令行，把每个参数转换为适当的类型然后调用相应的操作。


args.variables = [
              'current',\
              'active_power',\
              'reactive_power',\
              'winding_temperature',\
              'core_ground_current_data',\
              'H2_ppm',\
              'C2H4_ppm',\
              'C2H2_ppm',\
              'C2H6_ppm',\
              'CH4_ppm',\
              'CO_ppm',\
              'CO2_ppm',\
              'total_hydrocarbon_ppm',\
              'micro_water_ppm']  # 变量名
args.dbModelsNumber = [18, 6]  # 指定模型数据库的个数及参数个数
args.rank = ['IV', 'III', 'II', 'I']  # 分数等级，可修改个数及形式，如：['I', 'II', 'III']，['差', '一般', '正常', '良好']
args.scoreRange = [30, 110]  # 划分等级得分数范围，如只想让50-100的区间分数显示为[I-V]级，可以设置为[50.0,100.0]
myService = SERVICE(args)
                          #让myService与SERVICE(args)同等，SERVICE(args)是service文件中定义的一个类，此举使得myService也可调用SERVICE下的函数
                          #如run_transformer为SERVICE下的函数，myService.run_transformer() 使用它。
                          #SERVICE(args),args作为参数输入到SERVICE中，
# 定时执行函数
def re_exe(cmd, inc=60):
    print("间隔时间为：", inc)
    while True:
        os.system(cmd) #?? 功能：运行shell命令
        print("执行操作")
        myService.run_transformer() #启动主运行函数
        print("操作结束")
        print("等待下一次执行")
        time.sleep(inc)


if args.interval == 0:
    # 只执行一次
    print("执行程序")
    myService.run_transformer()
    print("程序结束")

else:
    re_exe("echo %time%", args.interval) #启动主运行函数
