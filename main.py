import pandas as pd

df = pd.read_excel('Orders.xlsx')
items_values = df['items'].unique()
packages_values = df['packages'].unique()

items = []
for k in packages_values:
    for i in items_values:
        if df[(df["items"] == i) & (df['packages'] == k)].empty:
            continue
        item = {'packages' : k, 'items' : i, 'warranty' : 'NO', 'duration' : 0 }
        dic = df[(df["items"] == i) & (df['packages'] == k)].to_dict(orient='records')
        for elt in dic:
            item[f'{elt['lables']}'] = elt['values']
        items.append(item)

for it in items:
    print(it)