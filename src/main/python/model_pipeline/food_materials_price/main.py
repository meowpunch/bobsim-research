from model_pipeline.food_materials_price.core import PriceModelPipeline


# Execute this pipeline offline to update feature extractors & model in AWS S3
def main():
    model_pipeline = PriceModelPipeline(
    )
    model_pipeline.process()


if __name__ == '__main__':
    main()
