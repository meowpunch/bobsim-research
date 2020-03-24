import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder


class FeatureExtractor:
    """
        This class will provide a vectorized form
    """

    @staticmethod
    def extract_from_date(X: pd.DataFrame):
        """
        :return: pd DataFrame that 'season' and 'is_weekend' is extracted
        """
        # add is_weekend column
        tmp_df = X.assign(
            is_weekend=lambda df: df.조사일자.dt.dayofweek.apply(
                lambda day: 1 if day > 4 else 0
            )
        )
        # add season column and drop date
        return tmp_df.assign(
            season=lambda df: df.조사일자.dt.month.apply(
                lambda month: (month % 12 + 3) // 3
            )
        ).drop("조사일자", axis=1)

    def __init__(self, X_train: pd.DataFrame, X_test: pd.DataFrame):
        self.x_train = self.extract_from_date(X_train)
        self.x_test = self.extract_from_date(X_test)

        # contain null value [강수 계속시간(hr), 일강수량(mm) -> 0
        numeric_features = [
            '평균기온(°C)', '최저기온(°C)', '최고기온(°C)', '강수 계속시간(hr)',
            '일강수량(mm)', '최대 풍속(m/s)', '평균 풍속(m/s)', '최소 상대습도(pct)',
            '평균 상대습도(pct)', '합계 일조시간(hr)', '합계 일사량(MJ/m2)',
            '평균 수온(°C)', '평균 최대 파고(m)']
        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
            ('scaler', StandardScaler())])

        categorical_features = ['품목명', '조사지역명', 'is_weekend', 'season']
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='error'))])

        self.column_transformer = ColumnTransformer(transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ])

    def fit_transform(self):
        # self.fit()
        train = self.column_transformer.fit_transform(self.x_train)
        test = self.column_transformer.transform(self.x_test)
        return train, test

# TODO: save
