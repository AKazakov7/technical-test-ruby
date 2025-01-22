import pandas as pd
from collections import OrderedDict


def order_list_dic(items, key_orders):
    for i in range(len(items)):
        items[i] = OrderedDict((key, items[i][key]) for key in key_orders)


def itemise_df(df):
    items_values = df['items'].unique()
    packages_values = df['packages'].unique()

    items = []
    for k in packages_values:
        for i in items_values:
            if df[(df["items"] == i) & (df['packages'] == k)].empty:
                continue
            item = {'packageID' : k, 'itemID' : i, 'warranty' : 'NO', 'duration' : 0, 'ref' : ""}
            dic = df[(df["items"] == i) & (df['packages'] == k)].to_dict(orient='records')
            for elt in dic:
                item[f'{elt['lables']}'] = elt['values']
            items.append(item)
    return items

df = pd.read_excel('Orders.xlsx')

items = itemise_df(df)

key_orders = ["itemID", "name", "price", "ref", "packageID", "warranty", "duration"]
order_list_dic(items, key_orders)

for it in items:
    print(it)