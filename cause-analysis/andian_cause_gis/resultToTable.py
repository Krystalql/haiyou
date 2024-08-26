from mysqlconfig import MysqlConfig
import pandas as pd
import pymysql
from logger import LOGGER

node_names = {
    'pd_peak': '放电峰值',
    'discharge_amplitude': '放电幅值',
    'discharge_times': '放电次数',
    'gis_pressure': '压力',
    'micro_water_content': '微水',
    'gis_temperature': '温度',
    'RiskLevel': '风险等级',
    'partial_discharge': '局放',
    'sf6': '油色谱'
}

mysqlConfig = MysqlConfig()


def clear_table(tabel_names):
    conn = pymysql.connect(host=mysqlConfig.HOST, user=mysqlConfig.USER,
                           password=mysqlConfig.PASSWORD, database=mysqlConfig.DATABASE, port=mysqlConfig.PORT)
    cursor = conn.cursor()
    for i in tabel_names:
        sql = f"DELETE FROM {i}"
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            LOGGER.error(exc)
    cursor.close()
    conn.close()


class ResultToTable:

    def __init__(self, id, name):
        self.mysqlConfig = MysqlConfig()
        self.id, self.name = id, name

    def cengci(self, shapley):
        """
        形成特征层和父节点之间的中间层layer2
        """
        layer2 = ['partial_discharge', 'sf6']

        # Layer2.
        pd_ = shapley.apply(lambda x: x['pd_peak'] + x['discharge_amplitude'] + x['discharge_times'], axis=1)
        sf_ = shapley.apply(lambda x: x['gis_pressure'] + x['micro_water_content'] + x['gis_temperature'], axis=1)

        layer2_df = pd.concat([pd_, sf_], axis=1)
        layer2_df.columns = layer2
        LOGGER.info("explain tree constructor done.")

        return layer2_df

    def query_node_id_4_layer(self, node_name):
        # 对node_name的每一个值，更换为对应的中文。

        global init_id
        node_name = node_names[f'{node_name}']
        conn = pymysql.connect(host=self.mysqlConfig.HOST, user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD, database=self.mysqlConfig.DATABASE,
                               port=mysqlConfig.PORT)
        cursor = conn.cursor()
        if node_name == 'last':
            sql = f"SELECT id FROM andian_gis_tree_explain ORDER BY id DESC LIMIT 1"
        else:
            sql = f"SELECT id FROM andian_gis_tree_explain WHERE node_name='{node_name}' ORDER BY id DESC LIMIT 1"
        try:
            cursor.execute(sql)
            init_id = cursor.fetchall()
            conn.commit()
        except Exception as exc:
            conn.rollback()
            LOGGER.error(exc)
        cursor.close()
        conn.close()

        return str(init_id[0][0])

    def write_layer_2_sql(self, table_name, layer_data):
        # 对node_name的每一个值，更换为对应的中文。

        layer_data = list(layer_data)
        layer_data[2] = node_names[f'{layer_data[2]}']
        layer_data = tuple(layer_data)

        # 写入数据库。
        conn = pymysql.connect(host=self.mysqlConfig.HOST, user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD, database=self.mysqlConfig.DATABASE,
                               port=mysqlConfig.PORT)
        cursor = conn.cursor()
        sql = f"INSERT INTO {table_name}(equip_id, equip_name, node_name, node_factor, parent_id) VALUES  " \
              f"(%s, %s, %s, %s, %s)"
        args = layer_data
        # cursor.execute(sql, args)
        # conn.commit()
        try:
            cursor.execute(sql, args)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            LOGGER.error(exc)
        cursor.close()
        conn.close()
        LOGGER.info("explain tree layer has been written.")

    def InsertShapToTable(self, shapley_values, tableName, y_train):

        # Get centi constructor of middle two layers.
        layer2 = self.cengci(shapley_values)

        # Write into mysql with formatted layer.
        layer_constructor = {'partial_discharge': ['pd_peak', 'discharge_amplitude', 'discharge_times'],
                             'sf6': ['micro_water_content', 'gis_temperature', 'gis_pressure']}

        # Decode risk_level column.
        layer_2_sql = [f"{self.id}", f"{self.name}", "RiskLevel", f"{y_train.values.tolist()[0]}", None]
        self.write_layer_2_sql(tableName, tuple(layer_2_sql))

        for node in list(layer_constructor.keys()):
            node_id = self.query_node_id_4_layer('RiskLevel')
            layer_2_sql = [f"{self.id}", f"{self.name}", f"{node}", f"{layer2[f'{node}'][0]}", f"{node_id}"]
            self.write_layer_2_sql(tableName, tuple(layer_2_sql))

            for sub_node in layer_constructor[f'{node}']:
                node_id = self.query_node_id_4_layer(f'{node}')
                layer_2_sql = [f"{self.id}", f"{self.name}", f"{sub_node}", f"{shapley_values[f'{sub_node}'][0]}",
                               f"{node_id}"]
                self.write_layer_2_sql(tableName, tuple(layer_2_sql))

    def InsertFeatureImportanceToTable(self, final_xgb, tableName):

        def write_xgb_2_sql(table_name, xgb_data):
            conn = pymysql.connect(host=self.mysqlConfig.HOST, user=self.mysqlConfig.USER,
                                   password=self.mysqlConfig.PASSWORD, database=self.mysqlConfig.DATABASE,
                                   port=mysqlConfig.PORT)
            cursor = conn.cursor()
            sql_sentence = f"INSERT INTO {table_name} VALUES"
            temp_str = ["%s," * len(xgb_data)]
            temp_str = f"({temp_str[0][:-1]})"
            sql_sentence += temp_str
            args = tuple(xgb_data)  # 对应表格的数据
            # cursor.execute(sql_sentence, args)  # 执行相关操作
            # conn.commit()  # 更新数据库
            try:
                cursor.execute(sql_sentence, args)  # 执行相关操作
                conn.commit()  # 更新数据库
            except Exception as exc:
                conn.rollback()
                LOGGER.error(exc)
            cursor.close()
            conn.close()

        write_xgb_2_sql(tableName, final_xgb)
        LOGGER.info("feature importance has been written.")

    def InsertMmdToTable(self, mmdRes, tableName):
        conn = pymysql.connect(host=self.mysqlConfig.HOST, user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD, database=self.mysqlConfig.DATABASE,
                               port=mysqlConfig.PORT)
        cursor = conn.cursor()
        sql_sentence = f"INSERT INTO {tableName} VALUES"
        temp_str = ["%s," * len(mmdRes.values)]
        temp_str = f"({temp_str[0][:-1]})"
        sql_sentence += temp_str
        args = tuple(mmdRes.values.tolist())  # 对应表格的数据
        try:
            cursor.execute(sql_sentence, args)  # 执行相关操作
            conn.commit()  # 更新数据库
        except Exception as exc:
            conn.rollback()
            LOGGER.error(exc)
        cursor.close()
        conn.close()
        LOGGER.info("mmd result has been written.")
