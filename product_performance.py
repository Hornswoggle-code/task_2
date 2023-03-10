import os

from dask import dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import analyze_lib as al

all_transactions = al.read_files()

holidays = dd.read_csv('holidays/all_holidays.csv')
holidays['Date'] = dd.to_datetime(holidays['Date'], dayfirst=True)
holidays = holidays['Date'].values.compute()

al.split_dates_holidays(all_transactions, holidays)

if not os.path.exists('performance_stats'):
    os.mkdir('performance_stats')

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

if not os.path.exists('plots'):
    os.mkdir('plots')

al.sum_values_groupby_c(all_transactions, 'Week', 'ActualSales').reset_index().plot(x='Week', y='ActualSales')
plt.savefig('plots/weekly_sales.png')

fig, ax = plt.subplots()
al.sum_values_groupby_c(all_transactions, 'Week', 'ActualSales').reset_index().plot(x='Week', y='ActualSales', ax=ax)
al.sum_values_groupby_c(all_transactions[all_transactions['SalesDiscount'] == 0], 'Week', 'ActualSales').reset_index()\
    .plot(x='Week', y='ActualSales', ax=ax)
plt.legend(['Weekly sales', 'Weekly sales without discount'])
plt.savefig('plots/weekly_sales_without_discount.png')

al.sum_values_groupby_c(all_transactions, 'Month', 'ActualSales').reset_index().plot(x='Month', y='ActualSales')
plt.savefig('plots/monthly_sales.png')

al.sum_values_groupby_c(all_transactions, 'Quarter', 'ActualSales').reset_index().plot(x='Quarter', y='ActualSales')
plt.savefig('plots/quarterly_sales.png')

al.sum_values_groupby_c(all_transactions, ['Week', 'Region_Lvl1'], 'ActualSales').unstack().plot()
plt.savefig('plots/regional_sales.png')

al.sum_values_groupby_c(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'ActualSales').unstack().plot()
plt.savefig('plots/category_sales.png')

al.sum_values_groupby_c(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'ActualSales').unstack().plot(kind='area')
plt.savefig('plots/category_sales2.png')

al.sum_values_groupby_c(all_transactions, ['Week', 'ProductCategory_Lvl2'], 'UnitVolume').unstack().plot()
plt.savefig('plots/volume_sales.png')

al.sum_values_groupby_c(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AA'], ['Week', 'ProductKey'], 'ActualSales').unstack().plot(kind='area', legend=False)
plt.savefig('plots/aa_sales.png')

al.sum_values_groupby_c(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AB'], ['Week', 'ProductKey'], 'ActualSales').unstack().plot(kind='area', legend=False)
plt.savefig('plots/ab_sales.png')

al.sum_values_groupby_c(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AC'], ['Week', 'ProductKey'], 'ActualSales').unstack().plot(kind='area', legend=False)
plt.savefig('plots/ac_sales.png')

al.sum_values_groupby_c(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AD'], ['Week', 'ProductKey'], 'ActualSales').unstack().plot(kind='area', legend=False)
plt.savefig('plots/ad_sales.png')

al.sum_values_groupby_c(all_transactions[all_transactions['ProductCategory_Lvl2'] == 'AE'], ['Week', 'ProductKey'], 'ActualSales').unstack().plot(kind='area', legend=False)
plt.savefig('plots/ae_sales.png')

al.sum_values_groupby_c(all_transactions[all_transactions['BrandKey'].isin([3521, 1713, 2435, 2146])], ['Week', 'BrandKey'], 'ActualSales').unstack().plot()
plt.savefig('plots/brand_sales.png')

al.mean_values_groupby(al.sum_values_groupby_c(all_transactions, ['TransactionDate', 'Week', 'WeekendFlag'], 'ActualSales').reset_index(),
                       ['Week', 'WeekendFlag'], 'ActualSales').unstack().plot()
plt.savefig('plots/workday_weekend_avg_sales.png')

fig, ax = plt.subplots()
al.sum_values_groupby_c(all_transactions, 'TransactionDate', 'ActualSales').reset_index().plot(x='TransactionDate', y='ActualSales', ax=ax, figsize=(12, 4))
for date in holidays:
    ax.axvline(x=date, linestyle='dashed', alpha=0.5)
plt.savefig('plots/holiday_sales.png')

fig, ax = plt.subplots()

monthly_sales = al.sum_values_groupby_c(all_transactions, 'Month', 'ActualSales')
monthly_sales_increase = (monthly_sales.diff() / (monthly_sales - monthly_sales.diff())).plot(x='Month', y='ActualSales', ax=ax)

monthly_cpi = pd.read_excel('cpi/monthly_cpi.xlsx')
monthly_cpi.rename(columns={'CPI_monthly ': 'CPI_monthly'}, inplace=True)
monthly_cpi['Avg_change'] = monthly_cpi['CPI_monthly'].diff() / (monthly_cpi['CPI_monthly'] - monthly_cpi['CPI_monthly'].diff())
monthly_increase = monthly_cpi.plot(x='Date_monthly', y='Avg_change', ax=ax)
del monthly_sales_increase
del monthly_increase
plt.savefig('plots/sales_increase_cpi.png')

print(al.mean_values_groupby(al.sum_values_groupby_c(all_transactions, ['TransactionDate', 'IsHoliday'], 'ActualSales').reset_index(), 'IsHoliday', 'ActualSales'))

