from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice


def main():
    open_data_raw_material_price = OpenDataRawMaterialPrice(
        bucket_name="production-bobsim",
        date="201908"
    )
    open_data_raw_material_price.process()


if __name__ == '__main__':
    main()
