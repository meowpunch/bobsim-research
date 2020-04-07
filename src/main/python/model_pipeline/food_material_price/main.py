from model_pipeline.food_material_price.core import PricePredictModelPipeline


def main():
    model_pipeline = PricePredictModelPipeline(
        bucket_name="production-bobsim",
    )
    model_pipeline.process(date="201908", data_process=True)


if __name__ == '__main__':
    main()
