from model_pipeline.food_material_price.core import PricePredictModelPipeline


def main():
    """
        process_type: only support for "production", "research"
    """
    model_pipeline = PricePredictModelPipeline(
        bucket_name="production-bobsim",
        logger_name="food_material_price_pipeline",
        date="201908"
    )
    model_pipeline.process(
        process_type="production",
        pipe_data=False
    )


if __name__ == '__main__':
    main()
