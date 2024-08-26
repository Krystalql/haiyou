from sklearn.model_selection import GridSearchCV
from dataProcess import ReadData, DataEncoder
from xgboost.sklearn import XGBRegressor


class XgboostTrain():

    def __init__(self, x_train, y_train, learning_rate=0.1, n_estimators=200, objective='reg:squarederror'
                 , max_depth=5, use_train_cv=False):
        self.xgb_model_ori = XGBRegressor(learning_rate=learning_rate, n_estimators=n_estimators, objective=objective
                                          , max_depth=max_depth)
        self.use_train_cv = use_train_cv

        if use_train_cv:
            grid_cv = GridSearchCV(self.xgb_model_ori,
                                   {'max_depth': [3, 4, 5, 6], 'subsample': [0.5, 0.6, 0.7, 0.8, 0.9]})
            grid_cv.fit(x_train, y_train)
            self.after_cv_params = grid_cv.best_params_
            self.xgb_model_ori.set_params(max_depth=grid_cv.best_params_['max_depth'],
                                          subsample=grid_cv.best_params_['subsample'])
            # print(grid_cv.best_params_)

        self.xgb_model_ori.fit(x_train, y_train)

    def get_param(self):
        return self.xgb_model_ori.get_params()

    def get_model(self):
        if self.use_train_cv:
            return self.xgb_model_ori
        else:
            return self.xgb_model_ori


def main():
    data_reader = ReadData('/Users/gap/Desktop/小样本致因分析/相关数据ori.xlsx', dl_split=True)
    data_train, data_test, label_train, label_test = data_reader.dataset_nd()

    encoder = DataEncoder(data_train)
    encoder1 = DataEncoder(data_test)
    data_train = encoder.encode_df()
    data_test = encoder1.encode_df()

    cls = XgboostTrain(x_train=data_train, y_train=label_train, n_estimators=200, use_train_cv=True)


if __name__ == '__main__':
    main()
