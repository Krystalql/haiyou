import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA


class ReadData():
    """
    Read data from file, then transfer to nd-array or DataFrame.
    """

    def __init__(self, filename, test_size=0.3, dl_split=False):
        self.data_read = pd.read_excel(filename)
        self.feature_name = self.data_read.columns.tolist()
        self.dl_split = dl_split

        data_read_nd = self.data_read.values
        data = data_read_nd[:, :-1]
        target = data_read_nd[:, -1]

        train_x, test_x, train_y, test_y = train_test_split(data, target, test_size=test_size)
        self.train_set = np.concatenate((train_x, np.transpose([train_y])), axis=1)
        self.test_set = np.concatenate((test_x, np.transpose([test_y])), axis=1)

        self.data_train, self.label_train = self.train_set[:, :-1], self.train_set[:, -1]
        self.data_test, self.label_test = self.test_set[:, :-1], self.test_set[:, -1]

    def dataset_nd(self):
        if self.dl_split:
            return self.data_train, self.data_test, self.label_train, self.label_test
        else:
            return self.train_set, self.test_set

    def dataset_df(self):
        if self.dl_split:
            self.data_train, self.data_test = pd.DataFrame(self.data_train), pd.DataFrame(self.data_test)
            self.label_train, self.label_test = pd.DataFrame(self.label_train), pd.DataFrame(self.label_test)
            return self.data_train, self.data_test, self.label_train, self.label_test
        else:
            train_set = pd.DataFrame(self.train_set, columns=self.feature_name)
            test_set = pd.DataFrame(self.test_set, columns=self.feature_name)
            return train_set, test_set


class DataEncoder():
    """
    Input: nd-array or DataFrame
    Output: nd-array or DataFrame after encode.
    """

    def __init__(self, data_ori):
        self.result = pd.DataFrame()
        self.init = LabelEncoder()
        self.data_ori = data_ori

        if isinstance(self.data_ori, np.ndarray):
            self.data_ori = pd.DataFrame(self.data_ori)

        if isinstance(self.data_ori, pd.DataFrame):
            for c in self.data_ori.columns:
                if self.data_ori[c].dtype == 'object':
                    if isinstance(self.data_ori[c].iloc[0], float):
                        temp_f = pd.DataFrame(self.data_ori[c]).columns.tolist()
                        temp = self.data_ori[c].values.tolist()
                        temp_str = [str(i) for i in temp]
                        self.data_ori[c] = pd.DataFrame(temp_str, columns=temp_f)
                    self.data_ori[c].fillna(f'{self.data_ori[c].iloc[0]}', inplace=True)
                    self.result[c] = self.init.fit_transform(self.data_ori[c])
                else:
                    self.data_ori[c] = self.data_ori[c].fillna(0)
                    self.result[c] = self.data_ori[c]

    def encode_np(self):
        return self.result.values

    def encode_df(self):
        return self.result


class DataStander():

    def __init__(self, data_ori):
        self.init = StandardScaler()
        self.data_std = self.init.fit_transform(data_ori)

    def stander_nd(self):
        return self.data_std.values

    def stander_df(self):
        return pd.DataFrame(self.data_std)


class DataMinMax():

    def __init__(self, data_ori):
        if isinstance(data_ori, np.ndarray):
            data_ori = pd.DataFrame(data_ori)
        self.init = MinMaxScaler()
        self.data_mm = self.init.fit_transform(data_ori)

    def mm_nd(self):
        return self.data_mm

    def mm_df(self):
        return pd.DataFrame(self.data_mm)


class UsePCA():

    def __init__(self, data_ori, n_components, use_stander=False):
        if use_stander:
            std = DataStander(data_ori)
            self.data = std.stander_df()

        self.data = data_ori
        pca = PCA(n_components=n_components)
        self.data_pca = pca.fit_transform(self.data)

    def pca_nd(self):
        return self.data_pca.values

    def pca_df(self):
        return pd.DataFrame(self.data_pca)


def main():
    pass


if __name__ == '__main__':
    main()
