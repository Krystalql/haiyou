import torch
import torch.nn as nn
import numpy as np


para = "insert into test_predict_tf_model_" + "1" + " (equip_id,equip_name,"
s = " values (%s,%s,"
for i in range(1, 6 + 1):
                para += "parameter" + str(i) + ","
                s += "%s,"
sql = "insert into test_transformer_predict (`equip_id`,\
                `equip_name`,`acquisition_time`,\
                `is_deleted`,\
                `voltage`,\
                `current`,\
                `apparent_power`,\
                `winding_temperature`,\
                `ae_partial_discharge`,\
                `rf_partial_discharge`,\
                `core_grounding_current`,\
                `score`,\
                `rank`)\
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
print(sql)