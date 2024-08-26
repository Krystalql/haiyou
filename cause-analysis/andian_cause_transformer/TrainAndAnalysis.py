import torch
from torch.autograd import Variable
from sklearn.ensemble import RandomForestRegressor as XGBRegressor
# from xgboost.sklearn import XGBRegressor
import pandas as pd
import shap
import numpy as np
from dataProcess import *
from logger import LOGGER
from mmd import mmd_rbf


def softmax(x, axis=1):
    # 计算每行的最大值
    row_max = x.max(axis=axis)

    # 每行元素都需要减去对应的最大值，否则求exp(x)会溢出，导致inf情况
    row_max = row_max.reshape(-1, 1)
    x = x - row_max

    # 计算e的指数次幂
    x_exp = np.exp(x)
    x_sum = np.sum(x_exp, axis=axis, keepdims=True)
    s = x_exp / x_sum
    return s


def mmd_compare(data, data_better, features_name, id, name, bench_id,
                bench_name):

    # features_name = features_name[15:]
    data = data.values
    data_better = data_better.values
    mmd_result = []
    for i in range(len(features_name)):
        temp = data[:, i]
        temp = temp.reshape(len(data), 1)
        temp_bet = data_better[:, i]
        temp_bet = temp_bet.reshape(len(data_better), 1)
        temp = temp.astype(float)
        temp_bet = temp_bet.astype(float)

        temp_x = torch.from_numpy(temp)
        temp_y = torch.from_numpy(temp_bet)
        temp_x, temp_y = Variable(temp_x), Variable(temp_y)
        mmd_temp_res = mmd_rbf(temp_x, temp_y)
        mmd_temp_res = float(mmd_temp_res)
        mmd_result.append(mmd_temp_res)
    data = data.astype('float')
    data_better = data_better.astype('float')
    x, y = torch.from_numpy(data), torch.from_numpy(data_better)
    x, y = Variable(x), Variable(y)
    all_mmd = float(mmd_rbf(x, y))
    mmd_result.append(all_mmd)
    mmd_result = pd.DataFrame(mmd_result)
    mmd_result.index = features_name + ['all_mmd']
    mmd_result = mmd_result.iloc[15:]
    mmd_result = mmd_result.fillna(0)

    core_cur = mmd_result.loc['core_ground_current_data'].values[0]
    temper = mmd_result.loc['winding_temperature'].values[0]
    oil = np.sum(mmd_result.loc['H2_ppm':'all_mmd'].values)

    mmd_result = mmd_result.fillna(0)
    mmd_result = pd.concat([mmd_result, pd.DataFrame([core_cur, temper, oil])])
    minmax_ = DataMinMax(mmd_result)
    mmdRes = minmax_.mm_df()
    prefix = pd.DataFrame([None, id, name, bench_id, bench_name])
    mmdRes = pd.concat([prefix, mmdRes])

    return mmdRes


class TrainAndAnalysis:
    def __init__(self, x_train, y_train, id, name):
        self.x_train, self.y_train = x_train, y_train
        self.name, self.id = name, id
        self.feature_name = pd.DataFrame(self.x_train).columns.tolist()[15:]
        xgbParams = {
            'n_estimators': 200,
        }
        self.model = XGBRegressor(**xgbParams)
        LOGGER.info(f"RF model init.")
        self.model.fit(x_train, y_train)
        LOGGER.info(f"RF model fitted.")

    def getShapleyAnalysis(self) -> pd.DataFrame:
        """
        :rtype: object
        """
        # Initialize the shapley explainer.
        # 去掉电参量
        explainer = shap.TreeExplainer(self.model)
        shapley_values = pd.DataFrame(
            explainer.shap_values(self.x_train)[:, 15:])
        # shapley_values = shapley_values * 100
        shapley_values = pd.DataFrame(softmax(
            shapley_values.values))  # 对shapely value做归一化，不然不好懂
        shapley_values.columns = self.feature_name
        LOGGER.info("Shapley analysis done.")

        return shapley_values

    def getFeatureImportance(self):
        # 得到XGBoost里的特征重要度，进行归一化。
        xgb_feature_importance = self.model.feature_importances_
        if sum(xgb_feature_importance) == 0:
            for i in range(len(xgb_feature_importance)):
                xgb_feature_importance[i] = 1
        xgb_feature_importance = pd.DataFrame(np.array(xgb_feature_importance))
        xgb_feature_importance = xgb_feature_importance.fillna(1)
        # minmax_ = DataMinMax(xgb_feature_importance)
        # xgb_feature_importance = minmax_.mm_df()
        xgb_feature_importance = xgb_feature_importance[15:]

        # 进行加和操作，形成层次结构。
        xgb_feature_importance.index = self.feature_name
        xgb_feature_importance.columns = [f'{self.id}']

        core_cur = xgb_feature_importance.loc[
            'core_ground_current_data'].values[0]
        temper = xgb_feature_importance.loc['winding_temperature'].values[0]
        oil = np.sum(xgb_feature_importance.loc['H2_ppm':].values)

        temp_xgb = [core_cur, temper, oil]
        temp_xgb = np.array(temp_xgb).reshape(-1, len(temp_xgb))[0]
        final_xgb = xgb_feature_importance.values.reshape(
            -1, len(xgb_feature_importance))[0]
        final_xgb = np.concatenate(
            [[None, self.id, self.name], final_xgb, temp_xgb], axis=0)
        LOGGER.info("feature importance exported.")

        return final_xgb
