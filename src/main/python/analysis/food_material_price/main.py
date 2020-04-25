from analysis.food_material_price.research import metric_by_test_size, metric_by_other_day
from util.build_dataset import build_master


def research_metric_by_test_size():
    dataset = build_master(bucket_name="production-bobsim", dataset="process_fmp", date="201908", pipe_data=False)

    metric_by_test_size(
        df=dataset, test_sizes=range(1, 10), train_size=10
    )


def research_metric_by_one_day():
    dataset = build_master(bucket_name="production-bobsim", dataset="process_fmp", date="201908", pipe_data=False)

    metric_by_other_day(
        df=dataset, train_size=5, test_size=1
    )


def main():
    # research_metric_by_test_size()
    research_metric_by_one_day()
    pass


if __name__ == '__main__':
    main()
