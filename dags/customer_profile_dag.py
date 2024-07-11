#datetime
from datetime import timedelta, datetime

# DAG object
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator

# Utils 
from utils.mysql_connection import table_df
import pandas as pd
from functools import reduce

# Initializing default arguments for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14)
}

# Instantiate a DAG
customer_profile_dag = DAG(
    dag_id='customer_profile_dag',
    default_args=default_args,
    description='This is a DAG to create a customer profile',
    schedule_interval='@once',
    catchup=False
)

def load_preprocess():
    rw_transaction_data = table_df('customer', 'rw_transaction_data')
    products = table_df('customer', 'products')
    product_category_map = table_df('customer', 'product_category_map')
    product_category_map = product_category_map.loc[:, ['product_id', 'module_id', 'product_type_id', 'txn_flow']]
    product_category_map.drop_duplicates(inplace=True)

    merged_df_1 = pd.merge(rw_transaction_data, products, on = ['product_id', 'product_type_id', 'module_id'], how = 'inner')
    df = pd.merge(merged_df_1, product_category_map, on = ['product_id', 'product_type_id', 'module_id'], how='inner')

    # Date
    df['last_modified_date'] = df['last_modified_date'].astype(str) 
    df['dates'] = pd.to_datetime(df['last_modified_date'] + df['time'], format='%Y-%m-%d%H:%M:%S')
    
    df[['last_modified_date', 'last_modified_date_bs', 'created_date']] = df[['last_modified_date', 'last_modified_date_bs', 'created_date']].apply(pd.to_datetime, errors='coerce')
    df['monthly'] = df['last_modified_date'].dt.month

    return df

load_preprocess_task = PythonOperator(
    task_id = 'load_preprocess_task',
    python_callable = load_preprocess,
    dag = customer_profile_dag
)

def aggregation(ti):

    df = ti.xcom_pull(task_ids='load_preprocess_task')
    #Txn Flow
    pivot_monthly = pd.pivot_table(df, index = ['payer_account_id', 'monthly'], columns = 'txn_flow', values = 'amount', aggfunc = 'sum').reset_index()
    aggregrated_df_monthly = (
        pivot_monthly
        .groupby('payer_account_id')
        .agg(
            total_inflow_amount = ('InFlow', 'sum'),
            total_outflow_amount = ('OutFlow', 'sum'),
            total_valuechain_amount = ('Value Chain', 'sum'),
            monthly_inflow_amount = ('InFlow', 'mean'),
            monthly_outflow_amount = ('OutFlow', 'mean'),
            monthly_valuechain_amount = ('Value Chain', 'mean'),
        )
        .reset_index()
    )
    pivot_monthly_count = pd.pivot_table(df, index = ['payer_account_id', 'monthly'], columns = 'txn_flow', values = 'amount', aggfunc = 'count').reset_index()
    aggregrated_df_monthly_count = (
        pivot_monthly_count
        .groupby('payer_account_id')
        .agg(
            total_inflow_count = ('InFlow', 'sum'),
            total_outflow_count = ('OutFlow', 'sum'),
            total_valuechain_count = ('Value Chain', 'sum'),
            monthly_inflow_count = ('InFlow', 'mean'),
            monthly_outflow_count = ('OutFlow', 'mean'),
            monthly_valuechain_count = ('Value Chain', 'mean'),
        )
        .reset_index()
    )
    ### Reward Points
    reward_agg = df.groupby('payer_account_id').agg({'reward_point': 'sum'})

    ### Lastest Transaction Date and Used Product
    idx_max_date = df.groupby('payer_account_id')['dates'].idxmax()
    latest_df = df.loc[idx_max_date, ['payer_account_id', 'product_name', 'dates']]
    latest_df.rename(columns = {'product_name': 'latest_used_product', 'dates': 'latest_tran_date'}, inplace=True)

    ### Revenue
    monthly_revenue= pd.pivot_table(df, index = ['payer_account_id', 'monthly'], values = 'revenue_amount', aggfunc = 'sum').reset_index()

    revenue = (
        monthly_revenue
        .groupby('payer_account_id')
        .agg(
            monthly_average_lifetime_revenue = ('revenue_amount', 'mean'),
            total_revenue = ('revenue_amount', 'sum'),
        )
        .reset_index()
    )

    ### This Month's Revenue
    latest_date = df['dates'].max()
    latest_month_df = df[df['dates'].dt.month == latest_date.month]

    this_month_revenue = latest_month_df.groupby('payer_account_id')['revenue_amount'].sum().reset_index(name = 'this_month_revenue')

    ### Product Usage
    product_usage = df.groupby('payer_account_id')['product_id'].count().reset_index().rename(columns={'product_id': 'product_usage'})

    # Nth Used Product
    product_counts = df.groupby(['payer_account_id', 'product_name'])['product_name'].count().reset_index(name='count')
    sorted_product_counts = product_counts.sort_values(by=['payer_account_id', 'count'], ascending=[True, False])

    most_used_product = sorted_product_counts.groupby('payer_account_id').nth(0).reset_index()
    second_most_used_product = sorted_product_counts.groupby('payer_account_id').nth(1).reset_index()
    third_most_used_product = sorted_product_counts.groupby('payer_account_id').nth(2).reset_index()
    nth_result = pd.merge(most_used_product, second_most_used_product, on='payer_account_id', suffixes=('_most', '_second'), how='left')
    nth_result = pd.merge(nth_result, third_most_used_product, on='payer_account_id', how='left')
    nth_result = nth_result[['payer_account_id', 'product_name_most', 'product_name_second', 'product_name']]
    nth_result.columns = ['payer_account_id', 'most_used_product', 'second_most_used_product', 'third_most_used_product']

    dfs = [aggregrated_df_monthly, aggregrated_df_monthly_count, latest_df, reward_agg, revenue, this_month_revenue, product_usage, nth_result]
    final_result = reduce(lambda left, right: pd.merge(left, right, on='payer_account_id'), dfs)
    final_result.to_csv('final_result.csv', header = True, index = False)
    return None


aggregation_task = PythonOperator(
    task_id = 'aggregation_task',
    python_callable = aggregation,
    dag = customer_profile_dag
)

load_preprocess_task >> aggregation_task












