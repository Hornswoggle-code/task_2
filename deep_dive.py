import sys
import matplotlib.pyplot as plt
import analyze_lib as al
import os


def deep_dive(product_key, transactions):
    if not os.path.exists(f'deep_dive_{product_key}'):
        os.mkdir(f'deep_dive_{product_key}')

    product_transactions = transactions[transactions['ProductKey'] == product_key].compute()
    product_values = product_transactions.iloc[0, [13, 14, 15]]
    brand_key, supplier_key, product_category_lvl2 = product_values
    other_products = transactions[(transactions['ProductCategory_Lvl2'] == product_category_lvl2)]
    print(f'Deep dive for ProductKey {product_key}:')
    print(product_values)

    fig, ax = plt.subplots()
    weekly_sales = al.sum_values_groupby(product_transactions, 'Week', 'ActualSales')
    weekly_sales.reset_index().plot(x='Week', y='ActualSales', title='Product sales vs. category sales', ax=ax)
    al.mean_values_groupby(al.sum_values_groupby_c(other_products, ['Week', 'ProductKey'], 'ActualSales').reset_index(), 'Week', 'ActualSales')\
        .reset_index().plot(x='Week', y='ActualSales', ax=ax)
    plt.legend([f'ProductKey {product_key}', f'Avg ProductCategory {product_category_lvl2}'])
    plt.savefig(f'deep_dive_{product_key}/sales_vs_category_{product_key}.png')

    (weekly_sales / al.sum_values_groupby_c(other_products, 'Week', 'ActualSales'))\
        .reset_index().plot(x='Week', y='ActualSales', title=f'Sales percentage in product category {product_category_lvl2}')
    plt.legend([f'ProductKey {product_key}'])
    plt.savefig(f'deep_dive_{product_key}/sales_percentage_{product_key}.png')

    fig, ax = plt.subplots()
    weekly_volume = al.sum_values_groupby(product_transactions, 'Week', 'UnitVolume')
    weekly_volume.reset_index().plot(x='Week', y='UnitVolume', title='Product volume vs. category volume', ax=ax)
    al.mean_values_groupby(al.sum_values_groupby_c(other_products, ['Week', 'ProductKey'], 'UnitVolume').reset_index(), 'Week', 'UnitVolume') \
        .reset_index().plot(x='Week', y='UnitVolume', ax=ax)
    plt.legend([f'ProductKey {product_key}', f'Avg ProductCategory {product_category_lvl2}'])
    plt.savefig(f'deep_dive_{product_key}/volume_vs_category_{product_key}.png')

    (weekly_volume / al.sum_values_groupby_c(other_products, 'Week', 'UnitVolume')) \
        .reset_index().plot(x='Week', y='UnitVolume',
                            title=f'UnitVolume percentage in product category {product_category_lvl2}')
    plt.legend([f'ProductKey {product_key}'])
    plt.savefig(f'deep_dive_{product_key}/volume_percentage_{product_key}.png')


transactions = al.read_files()
al.split_dates(transactions)

for product_key in sys.argv[1:]:
    deep_dive(int(product_key), transactions)

