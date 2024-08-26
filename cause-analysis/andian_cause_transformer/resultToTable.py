from mysqlconfig import MysqlConfig
import pymysql
from dataProcess import *
from mysql import Mysql
from logger import LOGGER

node_names = {
    'RiskLevel': '风险',
    'Electric': '电参量',
    'Voltage': '电压',
    'voltage_LVS_A': '低压侧电压-A相',
    'voltage_LVS_B': '低压侧电压-B相',
    'voltage_LVS_C': '低压侧电压-C相',
    'voltage_HVS_A': '高压侧电压-A相',
    'voltage_HVS_B': '高压侧电压-B相',
    'voltage_HVS_C': '高压侧电压-C相',
    'Current': '电流',
    'current_LVS_A': '低压侧电流-A相',
    'current_LVS_B': '低压侧电流-B相',
    'current_LVS_C': '低压侧电流-C相',
    'current_HVS_A': '高压侧电流-A相',
    'current_HVS_B': '高压侧电流-B相',
    'current_HVS_C': '高压侧电流-C相',
    'Power': '功率',
    'active_power': '有功功率',
    'reactive_power': '无功功率',
    'power_factor': '功率因数',
    'Winding_temperature': '绕组温度',
    'winding_temperature': '绕组温度',
    'Partial_discharge': '局部放电',
    'ae_partial_discharge': 'ae局部放电',
    'rf_partial_discharge': 'rf局部放电',
    'Core_grounding_current': '接地电流',
    'core_ground_current_data': '铁芯接地电流',
    'Oil_chromatography': '油色谱',
    'H2_ppm': 'H2含量',
    'C2H4_ppm': 'C2H4含量',
    'C2H2_ppm': 'C2H2含量',
    'C2H6_ppm': 'C2H6含量',
    'CH4_ppm': 'CH4含量',
    'CO_ppm': 'CO含量',
    'CO2_ppm': 'CO2含量',
    'total_hydrocarbon_ppm': '总烃含量',
    'micro_water_ppm': '微水含量',
}

mysqlConfig = MysqlConfig()


def clear_table(tabel_names):
    conn = pymysql.connect(host=mysqlConfig.HOST,
                           user=mysqlConfig.USER,
                           password=mysqlConfig.PASSWORD,
                           database=mysqlConfig.DATABASE,
                           port=mysqlConfig.PORT)
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
        layer2 = [
            'Winding_temperature', 'Core_grounding_current',
            'Oil_chromatography'
        ]

        # Layer2.
        wind_tem = shapley.apply(lambda x: x['winding_temperature'], axis=1)
        # par_disc = shapley.apply(lambda x: x['ae_partial_discharge'] + x['rf_partial_discharge'], axis=1)
        core = shapley.apply(lambda x: x['core_ground_current_data'], axis=1)
        oil = shapley.apply(lambda x: x['H2_ppm'] + x['C2H4_ppm'] + x[
            'C2H2_ppm'] + x['C2H6_ppm'] + x['CH4_ppm'] + x['CO_ppm'] + x[
                'CO2_ppm'] + x['total_hydrocarbon_ppm'] + x['micro_water_ppm'],
                            axis=1)

        layer2_df = pd.concat([wind_tem, core, oil], axis=1)
        layer2_df.columns = layer2
        LOGGER.info("explain tree constructor done.")

        return layer2_df

    def query_node_id_4_layer(self, node_name):
        # 对node_name的每一个值，更换为对应的中文。

        node_name = node_names[f'{node_name}']
        conn = pymysql.connect(host=self.mysqlConfig.HOST,
                               user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD,
                               database=self.mysqlConfig.DATABASE,
                               port=mysqlConfig.PORT)
        cursor = conn.cursor()
        if node_name == 'last':
            sql = f"SELECT id FROM andian_transformer_tree_explain ORDER BY id DESC LIMIT 1"
        else:
            sql = f"SELECT id FROM andian_transformer_tree_explain WHERE node_name='{node_name}' ORDER BY id DESC LIMIT 1"
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
        conn = pymysql.connect(host=self.mysqlConfig.HOST,
                               user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD,
                               database=self.mysqlConfig.DATABASE,
                               port=mysqlConfig.PORT)
        cursor = conn.cursor()
        sql = f"INSERT INTO {table_name}(equip_id, equip_name, node_name, node_factor, parent_id) VALUES  " \
              f"(%s, %s, %s, %s, %s)"
        args = layer_data
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
        layer_constructor = {
            'Winding_temperature': ['winding_temperature'],
            'Core_grounding_current': ['core_ground_current_data'],
            'Oil_chromatography': [
                'H2_ppm', 'C2H4_ppm', 'C2H2_ppm', 'C2H6_ppm', 'CH4_ppm',
                'CO_ppm', 'CO2_ppm', 'total_hydrocarbon_ppm', 'micro_water_ppm'
            ]
        }

        # Decode risk_level column.
        layer_2_sql = [
            f"{self.id}", f"{self.name}", "RiskLevel",
            f"{y_train.values.tolist()[0]}", None
        ]
        self.write_layer_2_sql('andian_transformer_tree_explain',
                               tuple(layer_2_sql))

        for node in list(layer_constructor.keys()):
            node_id = self.query_node_id_4_layer('RiskLevel')
            layer_2_sql = [
                f"{self.id}", f"{self.name}", f"{node}",
                f"{layer2[f'{node}'][0]}", f"{node_id}"
            ]
            self.write_layer_2_sql('andian_transformer_tree_explain',
                                   tuple(layer_2_sql))

            for sub_node in layer_constructor[f'{node}']:
                node_id = self.query_node_id_4_layer(f'{node}')
                layer_2_sql = [
                    f"{self.id}", f"{self.name}", f"{sub_node}",
                    f"{shapley_values[f'{sub_node}'][0]}", f"{node_id}"
                ]
                self.write_layer_2_sql(tableName, tuple(layer_2_sql))

    def InsertFeatureImportanceToTable(self, final_xgb, tableName):
        def write_xgb_2_sql(table_name, xgb_data):
            conn = pymysql.connect(host=self.mysqlConfig.HOST,
                                   user=self.mysqlConfig.USER,
                                   password=self.mysqlConfig.PASSWORD,
                                   database=self.mysqlConfig.DATABASE,
                                   port=mysqlConfig.PORT)
            cursor = conn.cursor()
            sql_sentence = f"INSERT INTO {table_name} VALUES"
            temp_str = ["%s," * len(xgb_data)]
            temp_str = f"({temp_str[0][:-1]})"
            sql_sentence += temp_str
            args = tuple(xgb_data)  # 对应表格的数据
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
        conn = pymysql.connect(host=self.mysqlConfig.HOST,
                               user=self.mysqlConfig.USER,
                               password=self.mysqlConfig.PASSWORD,
                               database=self.mysqlConfig.DATABASE,
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
