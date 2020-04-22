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
    model_pipeline.process(
        process_type="research",
        pipe_data=False
    )


if __name__ == '__main__':
    main()
