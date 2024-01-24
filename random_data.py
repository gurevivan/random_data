# Импорт библиотек
import random
import pandas as pd
from datetime import datetime

# Генерация даты создания ордера
def generate_date(start_date, end_date, num_orders):
    date_format = '%Y-%m-%d'
    start_time = datetime.strptime(start_date, date_format)
    end_time = datetime.strptime(end_date, date_format)

    delta = (end_time - start_time) / num_orders
    order_dates = [start_time + i * delta for i in range(num_orders)]

    return [date.strftime(date_format) for date in order_dates]

# Генерация Order ID
def generate_order_id(num_orders):
    return [i+1 for i in range(num_orders)]

# Генерация id товара
def generate_product_id(num_orders):
    return [random.randint(1, 5) for _ in range(num_orders)]

# Генерация id клиента
def generate_customer_id(num_orders):
    return [random.randint(1, 1000) for _ in range(num_orders)]

# Генерация статуса заказа
def generate_order_status(num_orders):
    return [random.randint(1, 1000) for _ in range(num_orders)]

# Генерация номера бригады
def generate_team_id(num_orders):
    return [random.randint(1, 3) for _ in range(num_orders)]

# Генерация дней изготовления
def generate_delta_work(num_orders):
    return [random.randint(10, 15) for _ in range(num_orders)]

# Генерация дней доставки
def generate_delta_delivery(num_orders):
    return [random.randint(3, 10) for _ in range(num_orders)]

# Количество заказов
num_orders = 1000

# Дата начала и окончания периода для создания заказов
start_date = '2023-01-01'
end_date = '2023-06-30'

# Генерация данных по заказам
date = generate_date(start_date, end_date, num_orders)
order_id = generate_order_id(num_orders)
order_status = 'open'
product_id = generate_product_id(num_orders)
team_id = generate_team_id(num_orders)
customer_id = generate_customer_id(num_orders)
order_status_end = generate_order_status(num_orders)
delta_work = generate_delta_work(num_orders)
delta_delivery = generate_delta_delivery(num_orders)

# Создание датафрейма open
open = pd.DataFrame({
    'date': date,
    'order_id': order_id,
    'order_status': order_status,
    'product_id': product_id,
    'team_id': team_id,
    'customer_id': customer_id,
    'order_status_end': order_status_end,
    'delta_work': delta_work,
    'delta_delivery': delta_delivery
})

# Преобразование типов в столюцах
open['date'] = pd.to_datetime(open['date'])
open['delta_work'] = pd.to_timedelta(open['delta_work'], unit='D')
open['delta_delivery'] = pd.to_timedelta(open['delta_delivery'], unit='D')

# Создание датафрейма made
made = open.query('order_status_end > 0')
made['date'] = made['date'] + made['delta_work']
made['order_status'] = 'made'

# Создание датафрейма defect
defect = made.query('order_status_end < 23')
defect['order_status'] = 'defect'

# Создание датафрейма delivered
delivered = made.query('order_status_end >= 23') 
delivered['date'] = delivered['date'] + delivered['delta_delivery']
delivered['order_status'] = 'delivered'

# Объединение датафреймов
df = pd.concat([open, made, defect, delivered]).sort_values('date').drop(['order_status_end', 'delta_work', 'delta_delivery'], axis= 1)

# Создание датафрейма teams
teams = pd.DataFrame({'team_id': [1, 2, 3],
                    'name': ['Бригада 1', 'Бригада 2', 'Бригада 3'],
                    'salary': [2000, 1900, 1700]})

# Создание датафрейма products
products = pd.DataFrame({'product_id': [1, 2, 3, 4, 5],
                       'product': ['Костюм 1', 'Костюм 2', 'Костюм 3',  'Костюм 4', 'Костюм 5'],
                       'expense': [2, 2.2, 2.3, 2.5, 3],
                       'price': [25000, 27000, 28000, 32000, 35000]})

# Создание датафрейма balance
balance = open.merge(products, how='left', on='product_id').groupby('date').agg({'expense': 'sum'}).reset_index()
balance['income'] = 0
mask = balance['date'].dt.is_month_start
balance.loc[mask, 'income'] += 450
balance['expense'] = balance['expense'].round(1)
balance['material'] = balance['expense'].cumsum()
balance['material1'] = balance['income'].cumsum()
balance['balance'] = (balance['material1'] - balance['material']).round(1)
balance['cost'] = balance['income'] * 1000 
balance = balance[['date', 'income', 'expense', 'balance', 'cost']]

# Сохранение в CSV файл
df.to_csv('orders_data.csv', index=False)
teams.to_csv('teams.csv', index=False)
products.to_csv('products.csv', index=False)
balance.to_csv('balance.csv', index=False)