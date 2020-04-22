from analysis.food_material_price.research import rmse_by_test_size
from util.build_dataset import build_master


def research_rmse_by_test_size():
    dataset = build_master(bucket_name="production-bobsim", dataset="process_fmp", date="201908", pipe_data=False)

    rmse_by_test_size(
        df=dataset, test_sizes=range(1, 10), train_size=10
    )


def main():
    research_rmse_by_test_size()


if __name__ == '__main__':
    main()
