import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge, Lasso, ElasticNet
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import PolynomialFeatures, StandardScaler, MinMaxScaler, OneHotEncoder


# TODO: move to jupyter notebook
class ModelSection:

    def __init__(self, X_data, y_target):
        self.X_data = X_data
        self.y_target = y_target

    def select_model(self, model_name, alpha:list =None, X_data_n=None, y_target_n=None, verbose=True):
        """
        TODO : think about this func's location and return anything or not
        :return: coeff_DataFrame
        """
        coeff_df = pd.DataFrame
        if verbose:
            print('########', model_name, '########')
        for param in alpha:
            if model_name == 'Ridge':
                model = Ridge(alpha=param)
            elif model_name == 'Lasso':
                model = Lasso(alpha=param)
            elif model_name == 'ElasticNet':
                model = ElasticNet(alpha=param, l1_ratio=0.7)

            neg_mse_scores = cross_val_score(model, X_data_n,
                                             y_target_n, scoring="neg_mean_squared_error", cv=5)
            avg_rmse = np.mean(np.sqrt(-1 * neg_mse_scores))

            print('alpha {0} : average of 5 Fold set RMSE : {1:.3f} '.format(param, avg_rmse))

            # extraction coeff because cross_val_score return only evaluation metric
            model.fit(self.X_data, self.y_target)

            # transform coeff each columns into Series and add to DataFrame's column
            coeff = pd.Series(data=model.coef_, index=self.X_data.columns)
            colname = 'alpha:' + str(param)
            coeff_df[colname] = coeff

            # view_coeff_per_alpha
            sort_column = 'alpha:' + str(alpha[0])
            coeff_df.sort_values(by=sort_column, ascending=False)

        return coeff_df

    def view_coeff_per_alpha(self, coeff_df):
            pass

    # TODO : change func's location to Preprocessing
    @staticmethod
    def get_scaled_data(method='None', p_degree=None, input_data=None):
        if method == 'Standard':
            scaled_data = StandardScaler().fit_transform(input_data)
        elif method == 'MinMax':
            scaled_data = MinMaxScaler().fit_transform(input_data)
        elif method == 'Log':
            scaled_data = np.log1p(input_data)
        elif method == 'Onehot':
            scaled_data = OneHotEncoder().fit_transform(input_data)
        else:
            scaled_data = input_data

        if p_degree is not None:
            scaled_data = PolynomialFeatures(degree=p_degree,
                                             include_bias=False).fit_transform(scaled_data)

        return scaled_data
