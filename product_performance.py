from dask import dataframe as dd
import matplotlib.pyplot as plt
import analyze_lib as al

all_transactions = al.read_files()

holidays = dd.read_csv('holidays/all_holidays.csv')
holidays['Date'] = dd.to_datetime(holidays['Date'], dayfirst=True)
holidays = holidays['Date'].values.compute()

all_transactions['Week'] = all_transactions['TransactionDate'].dt.to_period('W')
all_transactions['Month'] = all_transactions['TransactionDate'].dt.to_period('M')
all_transactions['Year'] = all_transactions['TransactionDate'].dt.to_period('Y')
all_transactions['Quarter'] = all_transactions['TransactionDate'].dt.to_period('Q')
all_transactions['IsHoliday'] = all_transactions['TransactionDate'].isin(holidays)

"""
al.sum_values_groupby_to_csv(all_transactions, 'Region_Lvl1', 'ActualSales', 'performance_stats/region_lvl1.csv')

al.sum_values_groupby_to_csv(all_transactions, 'Region_Lvl2', 'ActualSales', 'performance_stats/region_lvl2.csv')

al.sum_values_groupby_to_csv(all_transactions, 'ProductCategory_Lvl2', 'ActualSales', 'performance_stats/product_category_lvl2.csv')

al.sum_values_groupby_to_csv(all_transactions, 'BrandKey', 'ActualSales', 'performance_stats/brand_key.csv')
"""
al.sum_values_groupby(all_transactions, 'Week', 'ActualSales').reset_index().plot(x='Week', y='ActualSales')

al.sum_values_groupby(all_transactions, 'Month', 'ActualSales').reset_index().plot(x='Month', y='ActualSales')

al.sum_values_groupby(all_transactions, 'Quarter', 'ActualSales').reset_index().plot(x='Quarter', y='ActualSales')

al.sum_values_groupby(all_transactions, ['Week', 'Region_Lvl1'], 'ActualSales').unstack().plot()

al.sum_values_groupby(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'ActualSales').unstack().plot()

al.sum_values_groupby(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'UnitVolume').unstack().plot()

al.sum_values_groupby(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AD'], ['Week', 'ProductCategory_Lvl2'], 'UnitVolume').unstack().plot()

al.sum_values_groupby(all_transactions[all_transactions['BrandKey'].isin([3521, 1713, 2435, 2146])], ['Week', 'BrandKey'], 'UnitVolume').unstack().plot()

al.mean_values_groupby(al.sum_values_groupby(all_transactions, ['TransactionDate', 'Week', 'WeekendFlag'], 'ActualSales').reset_index(),
                       ['Week', 'WeekendFlag']).unstack().plot()

fig, ax = plt.subplots()
al.sum_values_groupby(all_transactions, 'TransactionDate', 'ActualSales').reset_index().plot(x='TransactionDate', y='ActualSales', ax=ax)
for date in holidays:
    ax.axvline(x=date, linestyle='dashed', alpha=0.5)

print(al.mean_values_groupby(al.sum_values_groupby(all_transactions, ['TransactionDate', 'IsHoliday'], 'ActualSales').reset_index(), 'IsHoliday'))

plt.show()
