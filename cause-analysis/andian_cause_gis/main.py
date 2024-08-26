from logger import LOGGER
from dataContain import DataContain
from trainAndAnalysis import TrainAndAnalysis
from resultToTable import ResultToTable
from trainAndAnalysis import mmd_compare
from resultToTable import clear_table
from tqdm import tqdm
import argparse
import time
from ymlHandler import YamlHandler


def main():
    # 清除所有之前的记录
    tableList = [
        'andian_gis_tree_explain', 'andian_gis_feature_importance',
        'andian_gis_mmd_good'
    ]
    clear_table(tableList)
    LOGGER.info("Init process [clear result table]: done.")

    DataContainer = DataContain()
    idList, nameIdPair = DataContainer.getNameIdPair()

    # 得到所有设备id name及数据，排除最佳设备
    bench_id = DataContainer.getBanchData()

    for idx in tqdm(range(len(idList))):
        id, name = idList[idx], nameIdPair.loc[nameIdPair['equip_id'] == idList[idx], 'equip_name'].values[0]
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
        writer.InsertShapToTable(sv, "andian_gis_tree_explain", y_train)
        writer.InsertFeatureImportanceToTable(fi,
                                              "andian_gis_feature_importance")
        if mmdRes is not None:
            writer.InsertMmdToTable(mmdRes, "andian_gis_mmd_good")


def parse_arg():
    parse = argparse.ArgumentParser(description='andian cause gis args')
    parse.add_argument("-t",
                       "--time",
                       type=int,
                       default=1,
                       help="time setting(min)")
    parse.add_argument("-i",
                       "--host",
                       type=str,
                       default='47.108.220.99',
                       help="host ip config")
    parse.add_argument("-d",
                       "--database",
                       type=str,
                       default='dqjc_zj_business',
                       help="name of database")
    parse.add_argument("-u",
                       "--user",
                       type=str,
                       default='dqjc_swpu',
                       help="user name")
    parse.add_argument("-w",
                       "--password",
                       type=str,
                       default='swpu@123456',
                       help="password")
    parse.add_argument("-p", "--port", type=int, default=23548, help="port")
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
        time.sleep(args.time * 60)
        print("GIS致因分析完成")
