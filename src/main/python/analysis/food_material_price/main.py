from analysis.food_material_price.research import metric_by_test_size, metric_by_other_term
from util.build_dataset import build_master


def main():
    dataset = build_master(bucket_name="production-bobsim", dataset="process_fmp", date="201908", pipe_data=False)

    # metric_by_test_size(df=dataset, test_sizes=range(1, 10), train_size=5)

    metric_by_other_term(df=dataset, train_size=10, test_size=2, n_days=range(8))
    pass


if __name__ == '__main__':
    main()
