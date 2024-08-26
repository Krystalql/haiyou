import numpy as np


class AHP:
    def __init__(self, criteria, b):
        self.RI = (0, 0, 0.58, 0.9, 1.12, 1.24, 1.32, 1.41, 1.45, 1.49,
                   1.52, 1.54, 1.56, 1.58, 1.59, 1.5943, 1.6064, 1.6213, 1.6302, 1.6374,
                   1.6416, 1.6471, 1.6527, 1.6556, 1.6599, 1.6626, 1.6682, 1.6714, 1.6728, 1.6748,
                   1.6776, 1.6801, 1.6822, 1.6846, 1.6856, 1.6865, 1.6888, 1.6901, 1.6918, 1.6938,
                   1.6951, 1.6968, 1.6979, 1.6993, 1.7008, 1.7023, 1.7029, 1.7042, 1.7051, 1.7061,
                   1.7071, 1.7076, 1.7086, 1.7098, 1.7115, 1.7118, 1.7125, 1.7126, 1.7134, 1.7147,
                   1.7149, 1.7157, 1.7165, 1.7168, 1.7173, 1.7176, 1.7182, 1.7192, 1.7197, 1.7203,
                   1.7204, 1.7207, 1.7214, 1.7219, 1.7224, 1.7228, 1.7232, 1.7236, 1.7243, 1.7245,
                   1.7250, 1.7251, 1.7255, 1.7260, 1.7264, 1.7270, 1.7274, 1.7275, 1.7278, 1.7281,
                   1.7282, 1.7283, 1.7288, 1.7293, 1.7297, 1.7300, 1.7301, 1.7302, 1.7305, 1.7309)
        self.criteria = criteria
        self.b = b
        self.num_criteria = criteria.shape[0]
        self.num_project = b[0].shape[0]

    def cal_weights(self, input_matrix):
        input_matrix = np.array(input_matrix)
        n, n1 = input_matrix.shape
        assert n == n1, '不是一个方阵'
        for i in range(n):
            for j in range(n):
                a = input_matrix[i, j]
                b = input_matrix[j, i]
                if np.abs(a * b - 1) > 1e-7:
                    raise ValueError('不是反互对称矩阵')

        # 一致性检验
        eigenValues, eigenVectors = np.linalg.eig(input_matrix)  # 计算特征值和特征向量
        maxIdx = np.argmax(eigenValues)
        maxEigen = eigenValues[maxIdx].real
        eigen = eigenVectors[:, maxIdx].real
        eigen = eigen / eigen.sum()

        CI = (maxEigen - n) / (n - 1)
        try:
            CR = CI / self.RI[n]
        except IndexError:
            CR = CI / self.RI[-1]
        return maxEigen, CR, eigen

    def run(self):
        # 准则层的最大特征，CR和最大特征向量
        maxEigen, CR, criteriaEigen = self.cal_weights(self.criteria)
        # print('准则层：最大特征值{:<5f},CR={:<5f},检验{}通过'.format(maxEigen, CR, '' if CR < 0.1 else '不'))
        # print('准则层权重={}\n'.format(criteriaEigen))

        max_eigen_list, CR_list, eigen_list = [], [], []
        for i in self.b:
            max_eigen, CR, eigen = self.cal_weights(i)
            max_eigen_list.append(max_eigen)
            CR_list.append(CR)
            eigen_list.append(eigen)

        # pd_print = pd.DataFrame(eigen_list,
        #                         index=['准则' + str(i) for i in range(self.num_criteria)],
        #                         columns=['方案' + str(i) for i in range(self.num_project)],
        #                         )
        # pd_print.loc[:, '最大特征值'] = max_eigen_list
        # pd_print.loc[:, 'CR'] = CR_list
        # pd_print.loc[:, '一致性检验'] = pd_print.loc[:, 'CR'] < 0.1
        # print('方案层')
        # print(pd_print)

        # 目标层
        obj = np.dot(criteriaEigen.reshape(1, -1), np.array(eigen_list))  # 目标层
        obj = np.squeeze(obj)
        return obj