import argparse
import time
from ahp_subcable import subcable_benchmark
from readDatabase import read_device
from readDatabase import read_weight
import writeDatabase
from data_processing import create_criteria
from readDatabase import read_target_time
from readDatabase import read_time
import sys


def parse_arg():
    parse = argparse.ArgumentParser(description='andian benchmark gis args')
    parse.add_argument("-t",
                       "--time",
                       type=int,
                       default=1,
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
    return parse.parse_args()


# test mysql
args = parse_arg()
config = {
    "host": args.host,
    "user": args.user,
    "password": args.password,
    "db": args.database,
    "port": args.port,
    "charset": "utf8"
}

while True:
    print("#" * 50)
    print("benchmark subcable start...")

    weight, flag = read_weight(config, "海缆")
    if flag != 0:
        print("程序异常")
        sys.exit(0)

    subcable_data, flag = read_device(config, "海缆")
    if flag != 0:
        print("程序异常")
        sys.exit(0)

    subcable_criteria = create_criteria(weight.values)

    # 读取目标时间
    target_ti, flag = read_target_time(config, "海缆")
    target_ti = target_ti[0][0]
    target_ti = target_ti.to_pydatetime()

    if flag != 0:
        print("程序异常")
        sys.exit(0)

    ti_all, flag = read_time(config, "海缆")
    if flag != 0:
        print("程序异常")
        sys.exit(0)
    ti_normal, ti_abnormal = ti_all

    is_run = False
    if ti_normal.empty and ti_abnormal.empty:
        is_run = True
    else:
        if (not ti_normal.empty) and (not ti_abnormal.empty):
            ti_normal = ti_normal[0][0]
            ti_abnormal = ti_abnormal[0][0]

            ti = ti_normal if ti_normal > ti_abnormal else ti_abnormal

        elif not ti_normal.empty:
            ti = ti_normal[0][0]
        else:
            ti = ti_abnormal[0][0]

        ti = ti.to_pydatetime()
        if ti != target_ti:
            is_run = True

    if is_run:
        toMysqlData_normal, toMysqlData_abnormal = subcable_benchmark(
            subcable_data, subcable_criteria, target_ti)
        writeDatabase.write_result(config, toMysqlData_normal,
                                   toMysqlData_abnormal, "海缆")
    time.sleep(args.time * 60)
