from resultToTable import clear_table
from dataContain import DataContain
from TrainAndAnalysis import TrainAndAnalysis
from resultToTable import ResultToTable
from TrainAndAnalysis import mmd_compare
from ymlHandler import YamlHandler
from logger import LOGGER
from tqdm import tqdm
import argparse
import time
import warnings

warnings.filterwarnings('ignore')


def main():
    # 清除之前的分析记录
    tableList = [
        'andian_transformer_tree_explain',
        'andian_transformer_feature_importance', 'andian_transformer_mmd_good'
    ]
    clear_table(tableList)
    LOGGER.info("Init process [clear result table]: done.")

    DataContainer = DataContain()
    idList, nameIdPair = DataContainer.getNameIdPair()

    ################## 标杆设备mmd对比 ###################
    # 得到所有设备id name及数据，排除最佳设备
    bench_id = DataContainer.getBanchData()

    for idx in tqdm(range(len(idList))):
        id, name = idList[idx], nameIdPair.loc[
            nameIdPair['equip_id'] == idList[idx], 'equip_name'].values[0]
        LOGGER.info(f"Processing equipment: {name}")

        # 训练数据整理
        x_train, y_train = DataContainer.getMonitorData(idList[idx])

        # 标杆设备数据处理
        mmdRes = None
        if idList[idx] == bench_id:
            pass
        else:
            bench_x, bench_y = DataContainer.getMonitorData(bench_id)
            mmdRes = mmd_compare(
                bench_x, x_train, x_train.columns.tolist(), id, name, bench_id,
                nameIdPair.loc[nameIdPair['equip_id'] == bench_id,
                               'equip_name'].values[0])

        # 初始化训练
        trainer = TrainAndAnalysis(x_train, y_train, id, name)
        sv = trainer.getShapleyAnalysis()
        fi = trainer.getFeatureImportance()

        # 结果写入到数据库
        writer = ResultToTable(id, name)
        writer.InsertShapToTable(sv, "andian_transformer_tree_explain",
                                 y_train)
        writer.InsertFeatureImportanceToTable(
            fi, "andian_transformer_feature_importance")
        if mmdRes is not None:
            writer.InsertMmdToTable(mmdRes, "andian_transformer_mmd_good")


def parse_arg():
    parse = argparse.ArgumentParser(description='andian cause trans args')
    parse.add_argument("-t",
                       "--time",
                       type=int,
                       default=0,
                       help="time setting(min)")
    parse.add_argument("-i",
                       "--host",
                       type=str,
                       default='127.0.0.1',
                       help="host ip config")
    parse.add_argument("-d",
                       "--database",
                       type=str,
                       default='dqjc_zj_business_4_8',
                       help="name of database")
    parse.add_argument("-u",
                       "--user",
                       type=str,
                       default='root',
                       help="user name")
    parse.add_argument("-w",
                       "--password",
                       type=str,
                       default='123456',
                       help="password")
    parse.add_argument("-p", "--port", type=int, default=3306, help="port")
    arg = parse.parse_args()

    # write ip INFO into ip.yml
    if arg.host:
        ymlF = YamlHandler("ip.yml")
        ymlF.write_yaml_data("HOST", arg.host)
        ymlF.write_yaml_data("DATABASE", arg.database)
        ymlF.write_yaml_data("USER", arg.user)
        ymlF.write_yaml_data("PASSWORD", arg.password)
        ymlF.write_yaml_data("PORT", arg.port)
        LOGGER.info("ip config from docker compose has been written into ip.yml")
        LOGGER.info("###### ip info as below:")
        LOGGER.info(f"## HOST: {arg.host}")
        LOGGER.info(f"## DATABSE: {arg.database}:")
        LOGGER.info(f"## USER: {arg.user}")
        LOGGER.info(f"## PASSWORD: {arg.password}")
        LOGGER.info("###### ip info")

    return arg


if __name__ == '__main__':
    args = parse_arg()
    while True:
        main()
        print("变压器致因分析完成")
        time.sleep(args.time * 60)