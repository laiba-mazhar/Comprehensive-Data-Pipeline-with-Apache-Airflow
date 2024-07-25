from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'olist_analysis_dag',
    default_args=default_args,
    description='Analyze the Olist E-Commerce Dataset',
    schedule_interval='@daily',
    start_date=datetime(2023, 7, 20),
    catchup=False,
)

data_path = '/home/laiba16/airflow/data/'

def extract_data(**kwargs):
    # Read all CSV files into Pandas DataFrames
    customers = pd.read_csv(os.path.join(data_path, 'olist_customers_dataset.csv'))
    geolocation = pd.read_csv(os.path.join(data_path, 'olist_geolocation_dataset.csv'))
    order_items = pd.read_csv(os.path.join(data_path, 'olist_order_items_dataset.csv'))
    order_payments = pd.read_csv(os.path.join(data_path, 'olist_order_payments_dataset.csv'))
    order_reviews = pd.read_csv(os.path.join(data_path, 'olist_order_reviews_dataset.csv'))
    orders = pd.read_csv(os.path.join(data_path, 'olist_orders_dataset.csv'))
    products = pd.read_csv(os.path.join(data_path, 'olist_products_dataset.csv'))
    sellers = pd.read_csv(os.path.join(data_path, 'olist_sellers_dataset.csv'))
    product_category_name_translation = pd.read_csv(os.path.join(data_path, 'product_category_name_translation.csv'))

    # Save DataFrames to temporary files for downstream tasks
    customers.to_pickle(os.path.join(data_path, 'customers.pkl'))
    geolocation.to_pickle(os.path.join(data_path, 'geolocation.pkl'))
    order_items.to_pickle(os.path.join(data_path, 'order_items.pkl'))
    order_payments.to_pickle(os.path.join(data_path, 'order_payments.pkl'))
    order_reviews.to_pickle(os.path.join(data_path, 'order_reviews.pkl'))
    orders.to_pickle(os.path.join(data_path, 'orders.pkl'))
    products.to_pickle(os.path.join(data_path, 'products.pkl'))
    sellers.to_pickle(os.path.join(data_path, 'sellers.pkl'))
    product_category_name_translation.to_pickle(os.path.join(data_path, 'product_category_name_translation.pkl'))

def preprocess_data(**kwargs):
    customers = pd.read_pickle(os.path.join(data_path, 'customers.pkl'))
    geolocation = pd.read_pickle(os.path.join(data_path, 'geolocation.pkl'))
    order_items = pd.read_pickle(os.path.join(data_path, 'order_items.pkl'))
    order_payments = pd.read_pickle(os.path.join(data_path, 'order_payments.pkl'))
    order_reviews = pd.read_pickle(os.path.join(data_path, 'order_reviews.pkl'))
    orders = pd.read_pickle(os.path.join(data_path, 'orders.pkl'))
    products = pd.read_pickle(os.path.join(data_path, 'products.pkl'))
    sellers = pd.read_pickle(os.path.join(data_path, 'sellers.pkl'))
    product_category_name_translation = pd.read_pickle(os.path.join(data_path, 'product_category_name_translation.pkl'))

    # Example preprocessing: Handling missing values
    orders = orders.dropna(subset=['order_purchase_timestamp'])
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

    # Save processed data
    orders.to_pickle(os.path.join(data_path, 'processed_orders.pkl'))

def analyze_data(**kwargs):
    orders = pd.read_pickle(os.path.join(data_path, 'processed_orders.pkl'))

    # Example analysis: Monthly order count
    orders['order_month'] = orders['order_purchase_timestamp'].dt.to_period('M')
    monthly_order_count = orders.groupby('order_month').size()

    # Plotting
    plt.figure(figsize=(10, 6))
    monthly_order_count.plot(kind='bar')
    plt.title('Monthly Order Count')
    plt.xlabel('Month')
    plt.ylabel('Number of Orders')
    plt.savefig(os.path.join(data_path, 'monthly_order_count.png'))

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

preprocess_data_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

extract_data_task >> preprocess_data_task >> analyze_data_task