from logger import LOGGER
from mysqlconfig import MysqlConfig
from mysql import Mysql
import pymysql
from dataProcess import *


class DataContain:

    def __init__(self):
        self.mysqlConfig = MysqlConfig()

        # Read data/header from mysql.
        sql_read = Mysql(host=self.mysqlConfig.HOST,
                         user=self.mysqlConfig.USER,
                         pwd=self.mysqlConfig.PASSWORD,
                         dbname=self.mysqlConfig.DATABASE,
                         port=self.mysqlConfig.PORT)
        self.sql_read = sql_read
        sql_sentence = self.mysqlConfig.selectMonitorAll
        self.monitorData, useless, header = sql_read.queryOperation(
            sql_sentence)

        # warning: 无数据
        if not len(self.monitorData) > 0:
            LOGGER.warning(
                f"No more new data at DATABASE {self.mysqlConfig.DATABASE}, sql sentence is: {sql_sentence}"
            )

        # Process to useful data.
        self.monitorData = pd.DataFrame(list(self.monitorData))
        self.monitorData.columns = header[0]
        self.monitorData.drop(columns='id', inplace=True)

        # Encode risk_level and type column.
        self.monitorData.loc[self.monitorData['risk_level'] == '无风险',
                             'risk_level'] = 0
        self.monitorData.loc[self.monitorData['risk_level'] == '低风险',
                             'risk_level'] = 1
        self.monitorData.loc[self.monitorData['risk_level'] == '中风险',
                             'risk_level'] = 2
        self.monitorData.loc[self.monitorData['risk_level'] == '高风险',
                             'risk_level'] = 3

        # clear all tables
        self.initTables()

    def clearTable(self, tabel_name):
        conn = pymysql.connect(host=self.mysqlConfig.HOST,
                               user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD,
                               database=self.mysqlConfig.DATABASE,
                               port=self.mysqlConfig.PORT)
        cursor = conn.cursor()
        for i in tabel_name:
            sql = f"DELETE from {i}"
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                LOGGER.error(exc)
        cursor.close()
        conn.close()

    def initTables(self):
        # Truncate tables in mysql to getting ready.
        table_list = [
            'andian_gis_feature_importance', 'andian_gis_tree_explain',
            'andian_gis_mmd_good'
        ]
        self.clearTable(table_list)

    def getNameIdPair(self):
        # Get all trans' names.
        trans_names = set(self.monitorData['equip_id'].values.tolist())
        trans_id_names = self.monitorData.iloc[:, :2].drop_duplicates()
        trans_id_list = list(trans_names)
        return trans_id_list, trans_id_names

    def getMonitorData(self, name):
        # copy self data
        monitorData = self.monitorData.copy()
        conn = pymysql.connect(host=self.mysqlConfig.HOST,
                               user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD,
                               database=self.mysqlConfig.DATABASE,
                               port=self.mysqlConfig.PORT)
        cur = conn.cursor()
        sql = self.mysqlConfig.selectMonitorById
        cur.execute(sql, name)

        monitorData.drop(columns=['pd_type', 'pd_phase'], inplace=True)

        data_4_train = monitorData.loc[monitorData['equip_id'] == name]
        data_4_train = data_4_train.drop(columns=['equip_id', 'input_time'],
                                         axis=1)
        # Nan 空值判断
        if data_4_train.isnull().values.any():
            data_4_train = data_4_train.fillna(0)
            LOGGER.warning(
                f"Nan exists in RF training data, processed with zeros.")
        x_train, y_train = data_4_train.iloc[:, 1:-2], data_4_train.iloc[:, -2]
        x_train = x_train.astype('float', copy=True, errors='raise')
        columns = x_train.columns
        minmax_ = DataMinMax(x_train)
        x_train = minmax_.mm_df()
        x_train.columns = columns

        return x_train, y_train

    def getBanchData(self):
        # Read benchmark device info.
        sql_sentence = self.mysqlConfig.selectBenchAll
        data_from_mysql_bm, _, header_bm = self.sql_read.queryOperation(
            sql_sentence)

        # warning: 无数据
        if not len(data_from_mysql_bm) > 0:
            LOGGER.warning(f"No new data in BenchMark results TABLE.")

        data_from_mysql_bm = pd.DataFrame(list(data_from_mysql_bm))
        data_from_mysql_bm.columns = header_bm[0]

        # 通过打分选取最好的设备
        # 得到最好的标杆设备名及其数据
        data_from_mysql_bm.sort_values(by='time',
                                       ascending=False,
                                       inplace=True)
        data_from_mysql_bm = data_from_mysql_bm.iloc[:14, :]
        data_from_mysql_bm.sort_values(by='score',
                                       ascending=False,
                                       inplace=True)
        best_trans = data_from_mysql_bm.iloc[0, 1]

        return best_trans
