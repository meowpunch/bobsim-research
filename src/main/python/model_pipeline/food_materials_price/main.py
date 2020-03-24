from model_pipeline.food_materials_price.core import PriceModelPipeline
from util.argparse import PipelineConfig


def main():

    model_pipeline = PriceModelPipeline()
    model_pipeline.process()


if __name__ == '__main__':
    main()
