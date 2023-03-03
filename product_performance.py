from dask import dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import analyze_lib as al

all_transactions = al.read_files()

holidays = dd.read_csv('holidays/all_holidays.csv')
holidays['Date'] = dd.to_datetime(holidays['Date'], dayfirst=True)
holidays = holidays['Date'].values.compute()

al.split_dates_holidays(all_transactions, holidays)


al.sum_values_groupby_to_csv(all_transactions, 'Region_Lvl1', 'ActualSales', 'performance_stats/region_lvl1.csv')

al.sum_values_groupby_to_csv(all_transactions, 'Region_Lvl2', 'ActualSales', 'performance_stats/region_lvl2.csv')

al.sum_values_groupby_to_csv(all_transactions, 'ProductCategory_Lvl2', 'ActualSales', 'performance_stats/product_category_lvl2.csv')

al.sum_values_groupby_to_csv(all_transactions, 'BrandKey', 'ActualSales', 'performance_stats/brand_key.csv')


al.sum_values_groupby_to_csv(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AA'], 'ProductKey',
                             'ActualSales', 'performance_stats/product_category_lvl2_aa.csv')

al.sum_values_groupby_to_csv(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AB'], 'ProductKey',
                             'ActualSales', 'performance_stats/product_category_lvl2_ab.csv')

al.sum_values_groupby_to_csv(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AC'], 'ProductKey',
                             'ActualSales', 'performance_stats/product_category_lvl2_ac.csv')

al.sum_values_groupby_to_csv(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AD'], 'ProductKey',
                             'ActualSales', 'performance_stats/product_category_lvl2_ad.csv')

al.sum_values_groupby_to_csv(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AE'], 'ProductKey',
                             'ActualSales', 'performance_stats/product_category_lvl2_ae.csv')


al.sum_values_groupby_c(all_transactions, 'Week', 'ActualSales').reset_index().plot(x='Week', y='ActualSales')

al.sum_values_groupby_c(all_transactions, 'Month', 'ActualSales').reset_index().plot(x='Month', y='ActualSales')

al.sum_values_groupby_c(all_transactions, 'Quarter', 'ActualSales').reset_index().plot(x='Quarter', y='ActualSales')

al.sum_values_groupby_c(all_transactions, ['Week', 'Region_Lvl1'], 'ActualSales').unstack().plot()

al.sum_values_groupby_c(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'ActualSales').unstack().plot()

al.sum_values_groupby_c(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'UnitVolume').unstack().plot()

al.sum_values_groupby_c(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AD'], ['Week', 'ProductCategory_Lvl2'], 'UnitVolume').unstack().plot()

al.sum_values_groupby_c(all_transactions[all_transactions['BrandKey'].isin([3521, 1713, 2435, 2146])], ['Week', 'BrandKey'], 'UnitVolume').unstack().plot()

al.mean_values_groupby(al.sum_values_groupby_c(all_transactions, ['TransactionDate', 'Week', 'WeekendFlag'], 'ActualSales').reset_index(),
                       ['Week', 'WeekendFlag'], 'ActualSales').unstack().plot()

fig, ax = plt.subplots()
al.sum_values_groupby_c(all_transactions, 'TransactionDate', 'ActualSales').reset_index().plot(x='TransactionDate', y='ActualSales', ax=ax)
for date in holidays:
    ax.axvline(x=date, linestyle='dashed', alpha=0.5)

fig, ax = plt.subplots()

monthly_sales = al.sum_values_groupby_c(all_transactions, 'Month', 'ActualSales')
monthly_sales_increase = (monthly_sales.diff() / (monthly_sales - monthly_sales.diff())).plot(x='Month', y='ActualSales', ax=ax)

monthly_cpi = pd.read_excel('cpi/monthly_cpi.xlsx')
monthly_cpi.rename(columns={'CPI_monthly ': 'CPI_monthly'}, inplace=True)
monthly_cpi['Avg_change'] = monthly_cpi['CPI_monthly'].diff() / (monthly_cpi['CPI_monthly'] - monthly_cpi['CPI_monthly'].diff())
monthly_increase = monthly_cpi.plot(x='Date_monthly', y='Avg_change', ax=ax)
del monthly_sales_increase
del monthly_increase

print(al.mean_values_groupby(al.sum_values_groupby_c(all_transactions, ['TransactionDate', 'IsHoliday'], 'ActualSales').reset_index(), 'IsHoliday'))

plt.show()
