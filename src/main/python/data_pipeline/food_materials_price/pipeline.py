from data_pipeline.main import DataPipeline


class PriceDataPipeline:
    """
        This will return a prepared pd DataFrame
    """

    def __init__(self):
        self.types = ['price', 'terrestrial_weather', 'marine_weather']

    def execute(self):
        self.process()

    def load(self):
        """
        :return: dictionary {str(name): pd DataFrame}
        """
        df_list = DataPipeline(
            args=self.types
        ).execute()
        return dict(zip(self.types, df_list))

    def process(self):
        self.load()
