import pandas as pd
from dask import dataframe as dd
import matplotlib.pyplot as plt
from datetime import date, datetime
import matplotlib.dates as mdates


def sum_values(dataframe1, dataframe2):
    return (dataframe1.iloc[:, [6, 5, 7]].sum() + dataframe2.iloc[:, [6, 5, 7]].sum()).compute()


def sum_values_groupby_to_csv(dataframe1, dataframe2, group, name):
    sum_values_groupby(dataframe1, dataframe2, group).to_csv(name)


def sum_values_groupby_sort(dataframe1, dataframe2, group):
    return sum_values_groupby(dataframe1, dataframe2, group).sort_values('ActualSales', ascending=False)


def sum_values_groupby(dataframe1, dataframe2, group):
    return (dataframe1.groupby(group)[["ActualSales", "UnitVolume", "SalesDiscount"]].sum() +
            dataframe2.groupby(group)[["ActualSales", "UnitVolume", "SalesDiscount"]].sum()).compute()


def mean_values_groupby(dataframe1, dataframe2, group):
    return (dataframe1.groupby(group)[["ActualSales", "UnitVolume", "SalesDiscount"]].sum() +
            dataframe2.groupby(group)[["ActualSales", "UnitVolume", "SalesDiscount"]].sum()).mean().compute()


def sum_values_groupby_to_csv_single(dataframe, group, name):
    sum_values_groupby_single(dataframe, group).to_csv(name)


def sum_values_groupby_single(dataframe, group):
    return dataframe.groupby(group)["ActualSales"].sum().compute()


def mean_values_groupby_single(dataframe, group):
    return dataframe.groupby(group)['ActualSales'].mean()


def get_season(now):
    now = now.replace(year=2000)
    return next(season for season, (start, end) in seasons
                if start <= now <= end)


no_promotion = dd.read_csv('final_data/transactions_no_promotion.csv/*')
no_promotion['TransactionDate'] = dd.to_datetime(no_promotion['TransactionDate'])
with_promotion = dd.read_csv('final_data/transactions_with_promotion.csv/*')
with_promotion['TransactionDate'] = dd.to_datetime(with_promotion['TransactionDate'])
all_transactions = dd.concat([no_promotion, with_promotion[no_promotion.columns]])

holidays = dd.read_csv('holidays/all_holidays.csv')
holidays['Date'] = dd.to_datetime(holidays['Date'], dayfirst=True)
holidays = holidays['Date'].values.compute()

#sum_values_groupby_to_csv_single(all_transactions, 'Region_Lvl1', 'performance_stats/region_lvl1.csv')

#sum_values_groupby_to_csv_single(all_transactions, 'Region_Lvl2', 'performance_stats/region_lvl2.csv')

#sum_values_groupby_to_csv_single(all_transactions, 'ProductCategory_Lvl2', 'performance_stats/product_category_lvl2.csv')

no_promotion['Week'] = no_promotion['TransactionDate'].dt.to_period('W')
with_promotion['Week'] = with_promotion['TransactionDate'].dt.to_period('W')
all_transactions['Week'] = all_transactions['TransactionDate'].dt.to_period('W')
all_transactions['Month'] = all_transactions['TransactionDate'].dt.to_period('M')

all_transactions['IsHoliday'] = all_transactions['TransactionDate'].isin(holidays)

seasons = {1: 'winter', 2: 'spring', 3: 'summer', 4: 'autumn'}
all_transactions['Season'] = (all_transactions['TransactionDate'].dt.month % 12 // 3 + 1).map(seasons)

fig, ax = plt.subplots()
sum_values_groupby_single(all_transactions, 'TransactionDate').reset_index().plot(x='TransactionDate', y='ActualSales', ax=ax)

for date in holidays:
    ax.axvline(x=date, linestyle='dashed', alpha=0.5)

"""
sum_values_groupby_single(all_transactions, 'Week').reset_index().plot(x='Week', y='ActualSales')
sum_values_groupby_single(all_transactions, 'Month').reset_index().plot(x='Month', y='ActualSales')
sum_values_groupby_single(all_transactions, ['Week', 'Region_Lvl1']).unstack().plot()
sum_values_groupby_single(all_transactions, ['Week', 'ProductCategory_Lvl2']).unstack().plot()
mean_values_groupby_single(sum_values_groupby_single(all_transactions, ['TransactionDate', 'Week', 'WeekendFlag']).reset_index(),
                           ['Week', 'WeekendFlag']).unstack().plot()
print(mean_values_groupby_single(sum_values_groupby_single(all_transactions, ['TransactionDate', 'IsHoliday']).reset_index(), 'IsHoliday'))
print(mean_values_groupby_single(sum_values_groupby_single(all_transactions, ['TransactionDate', 'Season']).reset_index(), 'Season'))
"""



plt.show()
