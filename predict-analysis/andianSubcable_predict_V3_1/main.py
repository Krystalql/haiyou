import time
import os
import argparse
from service import SERVICE

parser = argparse.ArgumentParser(description='LSTM-subCable predict')
parser.add_argument('--interval', type=int, required=False, default=1800,
                    help='Run interval')  # 脚本运行间隔,0表示只运行一次，其他数值表示间隔秒数
parser.add_argument('--variablesNumber', type=int, required=False, default=6, help='variablesNumber')  # 预测变量个数
parser.add_argument('--predictTimeStep', type=int, required=False, default=6, help='predictTimeStep')  # 预测未来总步数
parser.add_argument('--inputTimeStep', type=int, required=False, default=12, help='inputTimeStep')  # 一次输入步数
parser.add_argument('--outputTimeStep', type=int, required=False, default=1, help='outputTimeStep')  # 一次输出步数
parser.add_argument('--single_numLayers', type=int, required=False, default=1, help='single_numLayers')  # 单变量层数
parser.add_argument('--single_hidden_layer_size', type=int, required=False, default=64,
                    help='single_hidden_layer_size')  # 单变量隐藏层
parser.add_argument('--multi_numLayers', type=int, required=False, default=1, help='multi_numLayers')  # 多变量层数
parser.add_argument('--multi_hidden_layer_size', type=int, required=False, default=64,
                    help='multi_hidden_layer_size')  # 多变量隐藏层
parser.add_argument('--maxNormalValue', type=float, required=False, default=50000.0,
                    help='maxAbnormalValue')  # 最大正常值范围，超过该值系统进行容错
parser.add_argument('--minNormalValue', type=float, required=False, default=0.0,
                    help='maxAbnormalValue')  # 最小正常值范围，超过该值系统进行容错

# 数据库ip设置
parser.add_argument("-i",
                    "--host",
                    type=str,
                    default='127.0.0.1',
                    help="host ip config")
parser.add_argument("-d",
                    "--database",
                    type=str,
                    default='dqjc_zj_business',
                    help="name of database")
parser.add_argument("-u", "--user", type=str, default='root', help="user name")
parser.add_argument("-w",
                    "--password",
                    type=str,
                    default='wangziycy',
                    help="password")
parser.add_argument("-p", "--port", type=int, default=3306, help="port")

args = parser.parse_args()
args.variables = [
    'voltage', 'current', 'apparent_power', 'temperature_opti',
    'temperature_cableCore', 'disturbance_energy', 'score'
]  # 变量名
args.dbModelsNumber = [18, 6]  # 指定模型数据库的个数及参数个数
args.rank = ['II', 'III', 'II', 'I']  # 分数等级，可修改个数及形式，如：['I', 'II', 'III']，['差', '一般', '正常', '良好']
args.scoreRange = [30, 110.0]  # 划分等级得分数范围，如只想让50-100的区间分数显示为[I-V]级，可以设置为[50.0,100.0]
args.warn = [35.00, 281.00, 9835.00, 90.00, 90.00, 200.00]  # 数据库没存值的情况下的容错告警值。
myService = SERVICE(args)


# 定时执行函数
def re_exe(cmd, inc=60):
    print("间隔时间为：", inc)
    while True:
        os.system(cmd)
        print("执行操作")
        myService.run_transformer()
        print("操作结束")
        print("等待下一次执行")
        time.sleep(inc)


if args.interval == 0:
    # 只执行一次
    print("执行程序")
    myService.run_transformer()
    print("程序结束")
else:
    re_exe("echo %time%", args.interval)
