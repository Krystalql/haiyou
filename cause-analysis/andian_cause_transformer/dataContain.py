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

        # Encode risk_level column.
        self.monitorData.loc[self.monitorData['risk_level'] == '无风险',
                             'risk_level'] = 0
        self.monitorData.loc[self.monitorData['risk_level'] == '低风险',
                             'risk_level'] = 1
        self.monitorData.loc[self.monitorData['risk_level'] == '中风险',
                             'risk_level'] = 2
        self.monitorData.loc[self.monitorData['risk_level'] == '高风险',
                             'risk_level'] = 3

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
            'andian_transformer_feature_importance',
            'andian_transformer_tree_explain', 'andian_transformer_mmd_good'
        ]
        self.clearTable(table_list)

    def getNameIdPair(self):
        # Get all trans' names.
        conn = pymysql.connect(host=self.mysqlConfig.HOST,
                               user=self.mysqlConfig.USER,
                               passwd=self.mysqlConfig.PASSWORD,
                               database=self.mysqlConfig.DATABASE,
                               port=self.mysqlConfig.PORT)
        # sql = mysqlConfig.destSelectOld
        sql = self.mysqlConfig.srcSelectEquipId()
        cur = conn.cursor()
        cur.execute(sql, 11)
        gis_equip_info = cur.fetchall()
        des = cur.description
        header = [[item[0] for item in des]]
        # gis_equip_info, _, header = conn.queryOperation(sql)
        gis_equip_info = pd.DataFrame(np.array(gis_equip_info))
        gis_equip_info.columns = np.array(header)[0]

        # 对以往设备id进行整理 info = （id, name, time）
        idList = set(gis_equip_info['equip_id'].values.tolist())
        trans_id_names = gis_equip_info.iloc[:, :3]
        return list(idList), trans_id_names

    def getMonitorData(self, name):
        # copy self data
        monitorData = self.monitorData.copy()

        # 删除"视在功率"特征
        monitorData.drop('apparent_power', axis=1, inplace=True)

        data_4_train = monitorData.loc[monitorData['equip_id'] == name]
        data_4_train = data_4_train.drop(
            columns=['equip_id', 'acquisition_time'], axis=1)
        # Nan 空值判断
        if data_4_train.isnull().values.any():
            data_4_train = data_4_train.fillna(0)
            LOGGER.warning(
                f"Nan exists in RF training data, processed with zeros.")
        # 无此设备数据时
        if data_4_train.shape[0] == 0:
            LOGGER.warning(f"No data fo equip {name}")
        x_train, y_train = data_4_train.iloc[:, 1:-2], data_4_train.iloc[:, -2]
        x_train = x_train.astype('float', copy=True, errors='raise')
        columns = x_train.columns
        minmax_ = DataMinMax(x_train)
        x_train = minmax_.mm_df()
        x_train.columns = columns
        x_train = x_train.fillna(0)

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
