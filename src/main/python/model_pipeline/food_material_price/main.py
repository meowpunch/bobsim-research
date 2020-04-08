from model_pipeline.food_material_price.core import PricePredictModelPipeline


def main():
    model_pipeline = PricePredictModelPipeline(
        bucket_name="production-bobsim",
        logger_name="food_material_price_pipeline"
    )
    model_pipeline.process(date="201908", pipe_data=False)


if __name__ == '__main__':
    main()
