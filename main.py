import pandas as pd
import psycopg2
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
            item = {'packageid' : int(k), 'itemid' : int(i), 'warranty' : 'NO', 'duration' : 0, 'ref' : "", 'name' : "", 'price': 0}
            dic = df[(df["items"] == i) & (df['packages'] == k)].to_dict(orient='records')
            for elt in dic:
                item[f'{elt['lables']}'] = elt['values']
            items.append(item)
    return items

def add_order(cursor, order_name):
    cursor.execute("SELECT MAX(orderid) AS max_id FROM orders;")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    max_id += 1
    query = "INSERT INTO orders (orderid, odername) VALUES (%s, %s);"
    values = (max_id, order_name)
    cursor.execute(query, values)
    return max_id

def add_package(cursor, packageid, orderid):
    cursor.execute("SELECT MAX(packageid) AS max_id FROM packages;")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    max_id += 1
    query = "INSERT INTO packages (packageid, orderid) VALUES (%s, %s);"
    values = (max_id, orderid)
    cursor.execute(query, values)
    return max_id

def add_item(cursor, query, item):
    cursor.execute("SELECT MAX(itemid) AS max_id FROM items;")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    max_id += 1
    item["itemid"] = max_id

    if item["duration"] == "nan":
        item["duration"] = -1
    cursor.execute(query, item)

def build_item_query(item_dic):
    columns = ', '.join(item_dic.keys())
    placeholders = ', '.join([f"%({key})s" for key in item_dic.keys()]) 

    query = f"INSERT INTO items ({columns}) VALUES ({placeholders});"

    return query

file = 'Orders.xlsx'
files = pd.read_excel(file, sheet_name=None)
Orders = []
Orders_name = []
for file_name, df in files.items():

    Orders_name.append(file_name)
    items = itemise_df(df)

    key_orders = ["itemid", "name", "price", "ref", "packageid", "warranty", "duration"]
    order_list_dic(items, key_orders)

    Orders.append(items)

    print(file_name)
    for it in items:
        print(it)
    print()

try:
    connection = psycopg2.connect(
        dbname = "due",
        user = "due",
        password = "due",
        host = "localhost",
        port = "5432"
    )

    print("Opening new connection")

    cursor = connection.cursor()

    for i in range(len(Orders)):
        order_id = add_order(cursor, Orders_name[i])
        order = Orders[i]
        unique_package_id = set(d["packageid"] for d in order)
        for package_id in unique_package_id:
            new_package_id = add_package(cursor, package_id, order_id)
            for d in order:
                if d["packageid"] == package_id:
                    d["packageid"] = new_package_id
        for item in order:
            query = build_item_query(item)
            add_item(cursor, query, item)
        
        connection.commit()
        print(f"Added Order {Orders_name[i]}")


except Exception as e:
    print(e)

finally:
    if connection:
        cursor.close()
        connection.close()
    