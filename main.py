from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import aiohttp as aiohttp
import streamlit as st
import pandas as pd
import MySQLdb
import asyncio
import time
import io

connection = MySQLdb.connect(host='127.0.0.1', user='root', port=3306, password='password')
cursor = connection.cursor()


def add_st_elements(type, style, text):
    st.markdown("<" + type + " style=" + style + ": center; color: white;'>" + text + "</" + type + ">", unsafe_allow_html=True)


async def get_values(session):
    async with session.get('https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page') as raw_response:
        response = await raw_response.read()
    return [[url['href'][-15:-11], url['href'][-10:-8], url['href']] for url in BeautifulSoup(response, 'html.parser').find_all('a', {'title': 'Yellow Taxi Trip Records'})]


def select_values(values):
    selected_values = []
    for available_year in sorted(list(set([value[0] for value in values]))):
        if st.checkbox(available_year, key=available_year):
            available_months = [value[1] for value in values if value[0] == available_year]
            for x, col in enumerate(st.columns(len(available_months))):
                if col.checkbox(available_months[x], key=str(available_year) + ' ' + available_months[x]):
                    selected_values.append([available_year, available_months[x]])
    return selected_values


def select_urls(values, selected_values):
    selected_urls = []
    for selected_value in selected_values:
        for value in values:
            if selected_value[0] == value[0] and selected_value[1] == value[1]:
                selected_urls.append(value[2])
    return selected_urls


async def get_datas(session, url):
    async with session.get(url) as raw_response:
        response = await raw_response.read()
    if raw_response.status == 200:
        return io.BytesIO(response)
    else:
        return None


def write_to_database(data, choice):
    if choice == 'download':
        temp = []
        for element in data:
            temp.append(pd.read_parquet(element))
        dataframe = pd.concat(temp, axis=0, ignore_index=True)
    elif choice == 'upload':
        dataframe = pd.read_parquet(data)
    cursor.execute('CREATE DATABASE IF NOT EXISTS ppd')
    dataframe['date'] = dataframe['tpep_pickup_datetime'].astype(str).str[0:10]
    dataframe.to_sql(con=create_engine('mysql+mysqldb://root:password@127.0.0.1:3306/ppd'), name='yellow_tripdata', if_exists='replace')
    st.session_state['database'] = 'ok'


def check_database():
    cursor.execute('SHOW DATABASES')
    databases = [x[0] for x in cursor.fetchall()]
    if 'ppd' in databases:
        st.session_state['database'] = 'ok'


def reset_database():
    cursor.execute('DROP DATABASE ppd')
    for key in st.session_state.keys():
        del st.session_state[key]


def get_rows():
    cursor.execute('SELECT COUNT(*) FROM ppd.yellow_tripdata')
    return cursor.fetchall()[0][0]


def get_constraints():
    return {
        'airport_fee': {'type': 'float', 'values': '("0", "1.25")'},
        'congestion_surcharge': {'type': 'float'},
        'DOLocationID': {'type': 'int'},
        'extra': {'type': 'float'},
        'fare_amount': {'type': 'float'},
        'improvement_surcharge': {'type': 'float', 'values': '("0", "0.3")'},
        'mta_tax': {'type': 'float', 'values': '("0", "0.5")'},
        'passenger_count': {'type': 'int'},
        'payment_type': {'type': 'int', 'values': '("1", "2", "3", "4", "5", "6")'},
        'PULocationID': {'type': 'int'},
        'RatecodeID': {'type': 'int', 'values': '("1", "2", "3", "4", "5", "6")'},
        'store_and_fwd_flag': {'type': 'string', 'values': '("Y", "N")'},
        'tip_amount': {'type': 'float'},
        'tolls_amount': {'type': 'float'},
        'total_amount': {'type': 'float'},
        'tpep_dropoff_datetime': {'type': 'date', 'spec': '>= tpep_pickup_datetime'},
        'tpep_pickup_datetime': {'type': 'date', 'spec': '<= tpep_dropoff_datetime'},
        'trip_distance': {'type': 'float'},
        'VendorID': {'type': 'int', 'values': '("1", "2")'}
    }


def select_constraints():
    constraints = get_constraints()
    contraints = ['Ensemble'] + list(constraints.keys())
    temp = st.multiselect(label='', options=contraints)
    if 'Ensemble' in temp:
        return {key: value for key, value in constraints.items()}
    return {constraint: constraints[constraint] for constraint in temp}


def get_sql_typechecker(type, column):
    if type == 'int':
        return f'{column} NOT RLIKE "^[0-9]+$"'
    if type == 'float':
        return f'{column} NOT RLIKE "^[0-9]+\\.?[0-9]*$"'
    if type == 'date':
        return f'STR_TO_DATE({column}, "%Y-%m-%d %H:%i:%s") IS NULL'


def percentage(value, total):
    return round(100 - (value * 100 / total), 1)


def get_specific_result(constraints, nb_rows):
    result = {}
    for column, constraint in constraints.items():
        temp = {'completeness': 0, 'consistency': 0}
        temp['completeness'] = cursor.execute(f'SELECT * FROM ppd.yellow_tripdata WHERE {column} IS NULL')
        if constraint.get('values', None) is not None:
            temp['consistency'] += cursor.execute(f'SELECT * FROM ppd.yellow_tripdata WHERE {column} NOT IN {constraint["values"]}')
        elif constraint.get('type', None) is not None and constraint['type'] != 'string':
            temp['consistency'] += cursor.execute(f'SELECT * FROM ppd.yellow_tripdata WHERE {get_sql_typechecker(constraint["type"], column)}')
        if constraint.get('spec', None) is not None:
            temp['consistency'] += cursor.execute(f'SELECT * FROM ppd.yellow_tripdata WHERE !({column} {constraint["spec"]})')
        temp['total'] = f"{percentage(int(temp['completeness']) + int(temp['consistency']), nb_rows)}%"
        temp['completeness'] = f'{percentage(temp["completeness"], nb_rows)}%'
        temp['consistency'] = f'{percentage(temp["consistency"], nb_rows)}%'
        result[column] = temp
    return result


def percentage(value, total):
    return round(100 - (value * 100 / total), 1)


def get_full_result(nb_rows):
    result = {'completeness': 0, 'consistency': 0}
    completeness_query = 'SELECT * FROM ppd.yellow_tripdata WHERE 0=1'
    consistency_query = 'SELECT * FROM ppd.yellow_tripdata WHERE 0=1'
    for column, constraint in get_constraints().items():
        completeness_query += f' OR {column} IS NULL'
        for key, value in constraint.items():
            if key == 'values':
                consistency_query += f' OR {column} NOT IN {value}'
            if key == 'type' and value != 'string':
                consistency_query += f' OR {get_sql_typechecker(value, column)}'
            if key == 'spec':
                consistency_query += f' OR !({column} {value})'
    result['lignes'] = nb_rows
    result['completeness'] = cursor.execute(completeness_query)
    result['consistency'] = cursor.execute(consistency_query)
    result['completeness'] = f'{percentage(result["completeness"], nb_rows)}%'
    result['consistency'] = f'{percentage(result["consistency"], nb_rows)}%'
    return result


async def streamlit_main():
    st.set_page_config(layout='centered')
    add_st_elements('h1', 'left', "Evaluation de la qualit?? de donn??es")
    check_database()
    if 'database' not in st.session_state:
        async with aiohttp.ClientSession() as session:
            add_st_elements('h3', 'left', 'Base de donn??es inexistante')
            add_st_elements('h3', 'left', '\n')
            add_st_elements('h3', 'left', 'Choisissez les fichiers que vous souhaitez t??l??charger')
            add_st_elements('h3', 'left', '\n')
            values = await get_values(session)
            selected_urls = select_urls(values, select_values(values))
            add_st_elements('h3', 'left', 'Choisissez un fichier ?? charger')
            uploaded_file = st.file_uploader('', type='parquet', accept_multiple_files=False)
            add_st_elements('h3', 'left', '\n')
            if st.button('Suivant') or 'suivant' in st.session_state:
                add_st_elements('h3', 'left', '\n')
                if 'suivant' not in st.session_state:
                    st.session_state['suivant'] = 'ok'
                database_infos = st.empty()
                if not uploaded_file:
                    if selected_urls is not None:
                        database_infos.text('Telechargement des fichiers')
                        responses = await asyncio.gather(*[get_datas(session, selected_url) for selected_url in selected_urls])
                        database_infos.empty()
                        database_infos.text('Donn??es telecharg??es, chargement dans la base de donn??es')
                        write_to_database(responses, 'download')
                        database_infos.empty()
                        database_infos.text('Donn??es charg??es dans la base de donn??es')
                else:
                    database_infos.text('Donn??es telecharg??es, chargement dans la base de donn??es')
                    write_to_database(uploaded_file, 'upload')
                    database_infos.empty()
                    database_infos.text('Donn??es charg??es dans la base de donn??es')
                database_infos.empty()
    if 'database' in st.session_state:
        add_st_elements('h3', 'left', 'Base de donn??es existante')
        if st.button('Reinitialiser'):
            reset_database()
        if 'database' in st.session_state:
            add_st_elements('h3', 'left', '\n')
            add_st_elements('h3', 'left', 'Choisissez les attributs que vous souhaitez analyser')
            selected_constraints = select_constraints()
            add_st_elements('h3', 'left', '\n')
            if st.button('Analyser'):
                start = time.time()
                add_st_elements('h3', 'left', "R??sultats")
                add_st_elements('h4', 'left', "Analyse globale")
                nb_rows = get_rows()
                full_result = get_full_result(nb_rows)
                for x, col in enumerate(st.columns(len(full_result))):
                    col.metric(label=list(full_result)[x], value=list(full_result.values())[x])
                add_st_elements('h4', 'left', "Analyse sp??cifique")
                st.table(get_specific_result(selected_constraints, nb_rows))
                add_st_elements('p', 'left', str('{:.2f}'.format(time.time() - start)) + ' s pour analyser les donn??es')
                add_st_elements('h4', 'left', "Tableau des contraintes")
                st.json(get_constraints())
    connection.close()


asyncio.run(streamlit_main())
