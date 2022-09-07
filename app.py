from config import DBPostgres
import streamlit as st
import pyodbc
import pandas as pd
from pandas.api.types import is_numeric_dtype
import plotly.express as px

CONFIG = {
    "db_name": "store",
    "user": "Taulant",
    "password": "Kacavida1",
    "host": "serverstore.database.windows.net",
    "port": 8000
}

AGG_FUNCTIONS_OPT = ['COUNT', 'MIN', 'MAX', 'AVERAGE']
DATA_OPT = ['NONE', 'ORDERS', 'EMPLOYEES',  'PRODUCTS', 'ORDERS & EMPLOYEES', 'ORDERS & PRODUCTS']
LIMIT_OPT = ['NONE', 1, 5, 10, 50, 100]

MAP_DATA_OPT_DATAFRAME = {
    'ORDERS': 'orders',
    'EMPLOYEES': 'employees',
    'PRODUCTS': 'products',
    'ORDERS & EMPLOYEES': 'orders_employees',
    'ORDERS & PRODUCTS': 'orders_products'
}


def load_data_from_db():
    query_orders = f'''SELECT *
           FROM orders
           '''
    data_orders = db_instance.get_data(query_orders)

    query_orders_employee = f'''SELECT *
                  FROM orders
                  INNER JOIN employee
                  ON orders.employeeid = employee.employeeid
                  '''
    data_orders_employees = db_instance.get_data(query_orders_employee)

    query_orders_products = f'''SELECT *
                      FROM orders
                      INNER JOIN product
                      ON orders.productid = product.productid
                      '''
    data_orders_products = db_instance.get_data(query_orders_products)

    query_employees = f'''SELECT *
                          FROM employee
                          '''
    data_employees = db_instance.get_data(query_employees)

    query_products = f'''SELECT *
                             FROM product
                             '''
    data_products = db_instance.get_data(query_products)
    return data_orders, data_orders_employees, data_orders_products, data_employees, data_products


@st.cache(hash_funcs={pyodbc.Connection: lambda _: None})
def load_data():
    dict_data = {}
    data_orders, data_orders_employees, data_orders_products, data_employees, data_products = load_data_from_db()
    dict_data['orders'] = data_orders
    dict_data['orders_employees'] = data_orders_employees
    dict_data['orders_products'] = data_orders_products
    dict_data['employees'] = data_employees
    dict_data['products'] = data_products
    return dict_data


db_instance = DBPostgres()
db_instance.initialize(CONFIG)

st.set_page_config(page_title='Commerce', layout='wide')

st.markdown("""
        <style>
               .css-18e3th9 {
                    padding-top: 3rem;
                    padding-bottom: 10rem;
                    padding-left: 5rem;
                    padding-right: 5rem;
               }
               .css-1d391kg {
                    padding-top: 3.5rem;
                    padding-right: 1rem;
                    padding-bottom: 3.5rem;
                    padding-left: 1rem;
               }

               .stButton>button {
                    color: #000000;
                    border-radius: 10%;
                    backgroud-color: red !important;
                    height: 3em;
                    width: 10em;
               }
        </style>
        """, unsafe_allow_html=True)

data = load_data()

st.title("Commerce Overview 💰 💵 📊")

dataframe = st.selectbox(label="SELECT DATA", options=DATA_OPT)
df_to_show = None

t1, t2, t3, t4 = st.columns((1, 1, 1, 0.3))

group_by_opt = [] if dataframe == 'NONE' else list(data[MAP_DATA_OPT_DATAFRAME[dataframe]].columns)
order_by_opt = [] if dataframe == 'NONE' else list(data[MAP_DATA_OPT_DATAFRAME[dataframe]].columns)
group_by = t1.multiselect(label="GROUP BY FIELD", options=group_by_opt)
agg_attribute = t2.selectbox(label="AGGREGATION FIELD", options=['NONE'] + group_by_opt)
if dataframe == 'NONE':
    order_by = t3.selectbox(label="ORDER BY FIELD", options=[])
limit = t4.selectbox(label="LIMIT", options=LIMIT_OPT, index=3)

if dataframe != 'NONE':
    df_to_show = data[MAP_DATA_OPT_DATAFRAME[dataframe]]
    df_to_show = df_to_show.loc[:, ~df_to_show.columns.duplicated()].copy()
    if len(group_by) > 0 and agg_attribute != 'NONE':
        gb = df_to_show.groupby(group_by)
        counts = gb.size().to_frame(name='counts')
        if is_numeric_dtype(data[MAP_DATA_OPT_DATAFRAME[dataframe]][agg_attribute]):
            df_to_show = counts.join(gb.agg({agg_attribute: 'mean'}).rename(columns={agg_attribute: f'{agg_attribute}_MEAN'})
                                         .join(gb.agg({agg_attribute: 'median'}).rename(columns={agg_attribute: f'{agg_attribute}_MEDIAN'}))
                                         .join(gb.agg({agg_attribute: 'min'}).rename(columns={agg_attribute: f'{agg_attribute}_MIN'}))
                                         .join(gb.agg({agg_attribute: 'max'}).rename(columns={agg_attribute: f'{agg_attribute}_MAX'}))
                                         .join(gb.agg({agg_attribute: 'sum'}).rename(columns={agg_attribute: f'{agg_attribute}_SUM'}))).reset_index()
        else:
            df_to_show = counts
    if len(group_by) > 0 and agg_attribute == 'NONE':
        gb = df_to_show.groupby(group_by)
        counts = gb.size().to_frame(name='counts')
        df_to_show = counts
    order_by_opt = list(df_to_show.columns)
    order_by = t3.selectbox(label="ORDER BY FIELD", options=['NONE'] + order_by_opt)
    if order_by != 'NONE':
        df_to_show = df_to_show.sort_values(by=[order_by], ascending=False)
    df_to_show = df_to_show if limit == 'NONE' else df_to_show[0:limit]
    df_to_show = df_to_show.astype(str)

    col1, col2 = st.columns((1, 1))
    with col1:
        if 'counts' in list(df_to_show.columns):
            fig = px.histogram(data[MAP_DATA_OPT_DATAFRAME[dataframe]], x=group_by[0], histnorm='probability density')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if 'counts' in list(df_to_show.columns):
            df_to_show_pie = df_to_show.copy()
            df_to_show_pie = df_to_show_pie.reset_index()
            fig = px.pie(df_to_show_pie, values='counts', names=group_by[0], title='Population of European continent')
            st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df_to_show)
