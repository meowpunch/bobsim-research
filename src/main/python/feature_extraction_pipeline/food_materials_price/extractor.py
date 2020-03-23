import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class FeatureExtractor:
    """
        This class will provide a vectorized form
    """
    def __init__(self, prepared_data: pd.DataFrame):
        # contain null value [강수 계속시간(hr), 일강수량(mm) -> 0
        numeric_features = [
            '평균 기온(°C)', '최저기온(°C)', '최고기온(°C)', '강수 계속시간(hr)',
            '일강수량(mm)', '최대 풍속(m/s)', '평균 풍속(m/s)', '최소 상대습도(pct)',
            '평균 상대습도(pct)', '합계 일조시간(hr)', '합계 일사량(MJ/m2)',
            '평균 수온(°C)', '평균 최대 파고(m)']
        numeric_transform = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
            ('scaler', StandardScaler())])

        pass

    def fit(self):
        pass

    def transform(self):
        pass

    # TODO: save
