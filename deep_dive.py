from dask import dataframe as dd
import numpy as np
import matplotlib.pyplot as plt
import analyze_lib as al


def deep_dive(product_key, transactions):
    product_transactions = transactions[transactions['ProductKey'] == product_key].compute()
    print(f'ProductCategory: {product_transactions.iloc[0,:].co}')
    al.sum_values_groupby(product_transactions, 'TransactionDate', 'ActualSales').reset_index().plot(x='TransactionDate', y='ActualSales')
    plt.show()


all_transactions = al.read_files()

deep_dive(49340, all_transactions)
deep_dive(49341, all_transactions)
deep_dive(49333, all_transactions)
deep_dive(49329, all_transactions)
