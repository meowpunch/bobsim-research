from model_pipeline.public_data import make_origin_bucket


class ModelPipeline:

    def __init__(self):
        pass

    def execute(self):
        return self.process()

    def process(self):
        """
            TODO: logic comes here
        :return:
        """

        make_origin_bucket()
        pass
