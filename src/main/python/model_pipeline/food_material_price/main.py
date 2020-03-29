from model_pipeline.food_material_price.core import PricePredictModelPipeline


def main():
    model_pipeline = PricePredictModelPipeline(
        date="201908"
    )
    model_pipeline.process()


if __name__ == '__main__':
    main()
