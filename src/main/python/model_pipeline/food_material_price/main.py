from model_pipeline.food_material_price.core import PricePredictModelPipeline


def main():
    """
        tuned_process, untuned_process, search_process
    """
    model_pipeline = PricePredictModelPipeline(
        bucket_name="production-bobsim",
        logger_name="food_material_price_pipeline",
        date="201908"
    )
    model_pipeline.search_process(pipe_data=True)


if __name__ == '__main__':
    main()
