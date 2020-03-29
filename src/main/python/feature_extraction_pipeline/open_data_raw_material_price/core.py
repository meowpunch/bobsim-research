from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import QuantileTransformer, OneHotEncoder

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class RawMaterialPriceExtractor:
    def __init__(self):
        self.logger = init_logger()

        # s3
        self.bucket_name = "production-bobsim"
        self.file_name = "201908.csv"
        self.load_key = "public_data/open_data_raw_material_price/process/csv/{filename}".format(
            filename=self.file_name
        )

        self.x_train = None
        self.x_test = None

        numeric_features = []
        numeric_transformer = Pipeline(steps=[
            # TODO: consider about imputer and reduce_dim
            # ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
            ('scaler', QuantileTransformer())
        ])

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

    def load(self):
        """
            fetch DataFrame and astype and filter by columns
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_objects(key=self.load_key)

        # TODO: no use index to get first element.
        return df[0]