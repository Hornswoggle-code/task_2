from dask import dataframe as dd


def read_files():
    no_promotion = dd.read_csv('final_data/transactions_no_promotion.csv/*')
    no_promotion['TransactionDate'] = dd.to_datetime(no_promotion['TransactionDate'])
    with_promotion = dd.read_csv('final_data/transactions_with_promotion.csv/*')
    with_promotion['TransactionDate'] = dd.to_datetime(with_promotion['TransactionDate'])
    return dd.concat([no_promotion, with_promotion[no_promotion.columns]])


def sum_values_groupby_to_csv(dataframe, group, column, name):
    sum_values_groupby_sort(dataframe, group, column).to_csv(name)


def sum_values_groupby_sort(dataframe, group, column):
    return sum_values_groupby(dataframe, group, column).reset_index().sort_values(column, ascending=False)


def sum_values_groupby(dataframe, group, column):
    return dataframe.groupby(group)[column].sum().compute()


def mean_values_groupby(dataframe, group):
    return dataframe.groupby(group)['ActualSales'].mean()