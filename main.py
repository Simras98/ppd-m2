from sqlalchemy import create_engine
from datetime import datetime
from bs4 import BeautifulSoup
import aiohttp as aiohttp
import streamlit as st
import pandas as pd
import cryptography
import requests
import asyncio
import pymysql
import time
import io


def add_st_elements(type, style, text):
    st.markdown("<" + type + " style=" + style + ": center; color: white;'>" + text + "</" + type + ">", unsafe_allow_html=True)


def get_values():
    values = []
    soup = BeautifulSoup(requests.get('https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page').text, 'html.parser')
    for div in soup.find_all('div', class_='faq-answers')[0:]:
        for td in div.find('table').find('tbody').find('tr').find_all('td'):
            for row in td.contents:
                if row.name in ['b', 'p']:
                    values.append([div['id'][3:].replace('\n', ''), row.getText().replace('\n', '').replace(' ', ''), None])
                elif row.name == 'ul':
                    values[-1][2] = row.find('a', {'title': 'Yellow Taxi Trip Records'})['href']
    return values


def select_values(values):
    selected_values = []
    for available_year in sorted(list(set([value[0] for value in values]))):
        if st.checkbox(available_year, key=available_year):
            available_months = [value[1] for value in values if value[0] == available_year]
            for x, col in enumerate(st.columns(len(available_months))):
                if col.checkbox(str(datetime.strptime(available_months[x], "%B").month), key=str(available_year) + ' ' + available_months[x]):
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
        return io.StringIO(response.decode('utf-8'))
    else:
        return None


def write_to_database(data, choice):
    if choice == 'download':
        temp = []
        for element in data:
            temp.append(pd.read_csv(element, index_col=None, header=0, dtype=str))
        dataframe = pd.concat(temp, axis=0, ignore_index=True)
    elif choice == 'upload':
        dataframe = pd.read_csv(data, index_col=None, header=0, dtype=str)
    dataframe["Date"] = dataframe["tpep_pickup_datetime"].str[:7]
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password')
    with db_connection.cursor() as cursor:
        cursor.execute('CREATE DATABASE IF NOT EXISTS ppd')
    db_connection.close()
    db_connection = create_engine('mysql+pymysql://root:password@127.0.0.1/ppd', pool_recycle=3600).connect()
    dataframe.to_sql('yellow_tripdata', db_connection, if_exists='replace')
    db_connection.close()
    st.session_state['database'] = 'ok'


def check_database():
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password')
    with db_connection.cursor() as cursor:
        cursor.execute('SHOW DATABASES')
        databases = [x[0] for x in cursor.fetchall()]
    db_connection.close()
    if 'ppd' in databases:
        st.session_state['database'] = 'ok'


def reset_database():
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password')
    with db_connection.cursor() as cursor:
        cursor.execute('DROP DATABASE ppd')
    db_connection.close()
    del st.session_state['database']


def get_columns():
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password', database='ppd')
    with db_connection.cursor() as cursor:
        cursor.execute('SHOW COLUMNS from yellow_tripdata')
        columns = [x[0] for x in cursor.fetchall() if x[0] not in ['index', 'Date']]
    db_connection.close()
    return columns


def get_rows():
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password', database='ppd')
    with db_connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM yellow_tripdata')
        rows = cursor.fetchall()[0][0]
    db_connection.close()
    return rows


def get_constraints():
    return {
        'congestion_surcharge': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['', '']],
        'DOLocationID': [['int', "N'est pas de type int"], ['none', 'Est vide'], ['', '']],
        'extra': [['float', "N'est pas de type float"], ['none', 'Est vide'], [['0.5', '1.0'], 'N appartient pas à [0.5, 1.0]']],
        'fare_amount': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['> 0', 'N est pas > 0']],
        'improvement_surcharge': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['', '']],
        'mta_tax': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['= 0.5', 'N est pas = 0.5']],
        'passenger_count': [['int', "N'est pas de type int"], ['none', 'Est vide'], ['>= 1', 'N est pas >= 1']],
        'payment_type': [['int', "N'est pas de type int"], ['none', 'Est vide'], [['1', '2', '3', '4', '5', '6'], 'N appartient pas à [1, 2, 3, 4, 5, 6]']],
        'PULocationID': [['int', "N'est pas de type int"], ['none', 'Est vide'], ['', '']],
        'RatecodeID': [['int', "N'est pas de type int"], ['none', 'Est vide'], [['1', '2', '3', '4', '5', '6'], 'N appartient pas à [1, 2, 3, 4, 5, 6]']],
        'store_and_fwd_flag': [['str', "N'est pas de type str"], ['none', 'Est vide'], [['Y', 'N'], 'N appartient pas à [Y, N]']],
        'tip_amount': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['>= 0', 'N est pas >= 0']],
        'tolls_amount': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['>= 0', 'N est pas >= 0']],
        'total_amount': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['>= 0', 'N est pas >= 0']],
        'tpep_dropoff_datetime': [['date', "N'est pas de type date"], ['none', 'Est vide'], ['> tpep_pickup_datetime', 'N est pas > tpep_pickup_datetime']],
        'tpep_pickup_datetime': [['date', "N'est pas de type date"], ['none', 'Est vide'], ['< tpep_dropoff_datetime', 'N est pas < tpep_dropoff_datetime']],
        'trip_distance': [['float', "N'est pas de type float"], ['none', 'Est vide'], ['>= 0', 'N est pas >= 0']],
        'VendorID': [['int', "N'est pas de type int"], ['none', 'Est vide'], [['1', '2'], 'N appartient pas à [1, 2]']]}


def select_constraints(columns):
    constraints = get_constraints()
    selected_constraints = {}
    for column in columns:
        if st.checkbox(column, key=column):
            column_constraints = [element[1] for element in constraints[column]]
            for x, col in enumerate(st.columns(len(column_constraints))):
                if col.checkbox(str(column_constraints[x]), key=str(column) + ' ' + str(column_constraints[x])):
                    if column not in selected_constraints:
                        selected_constraints[column] = [''] * 3
                        selected_constraints[column][x] = constraints[column][x][0]
                    else:
                        selected_constraints[column][x] = constraints[column][x][0]
    return selected_constraints


def get_result(column, contraint):
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password', database='ppd')
    with db_connection.cursor() as cursor:
        if contraint == '':
            return ''
        if contraint in ['< tpep_dropoff_datetime', '> tpep_pickup_datetime']:
            return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE NOT ' + column + ' ' + contraint.split()[0] + '  ' + contraint.split()[1])
        if contraint == 'float':
            return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' NOT RLIKE "^[0-9]+\\.?[0-9]*$"')
        if contraint == 'int':
            return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' NOT RLIKE "^[0-9]+$"')
        if contraint == 'date':
            return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE STR_TO_DATE(' + column + ', "%D,%M,%Y") IS NOT NULL')
        if contraint == 'none':
            return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' IS NULL')
        if type(contraint) is list:
            if column == 'extra':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' != 0.5 AND ' + column + ' != 1.0')
            if column in ['payment_type', 'RatecodeID']:
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' < 1 OR ' + column + ' > 6')
            if column == 'store_and_fwd_flag':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' NOT LIKE "Y" AND ' + column + ' NOT LIKE "N"')
            if column == 'VendorID':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' != 1 AND ' + column + ' != 2')
        if contraint in ['>= 0', '>= 1', '> 0', '> 1', '= 0.5']:
            compared_value = contraint.split()[1]
            comparator = contraint.split()[0]
            if comparator == '>=':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' < ' + compared_value)
            if comparator == '>':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' <= ' + compared_value)
            if comparator == '<':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' => ' + compared_value)
            if comparator == '=':
                return cursor.execute('SELECT ' + column + ' FROM yellow_tripdata WHERE ' + column + ' != ' + compared_value)


def analyse(contraints):
    result = {}
    for column in contraints.keys():
        result[column] = [None] * 3
        for x, constraint in enumerate(contraints[column]):
            result[column][x] = get_result(column, constraint)
    return result


def display_result(result, rows):
    constraints = get_constraints()
    for key, value in result.items():
        result[key] = [constraints[key][x][1] + ' : ' + str(element) if element != '' else '' for x, element in enumerate(value)]
    st.markdown("""<style>.row_heading.level0 {display:none}.blank {display:none}</style>""", unsafe_allow_html=True)
    st.table(result)


async def streamlit_main():
    st.set_page_config(layout='centered')
    add_st_elements('h1', 'left', "Application d'évaluation de qualité")
    check_database()
    if 'database' not in st.session_state:
        add_st_elements('h3', 'left', 'Base de données non initialisée, choisissez comment vous souhaitez la completer')
        add_st_elements('h3', 'left', '\n')
        add_st_elements('h3', 'left', 'Choisissez les fichiers que vous souhaitez télécharger')
        add_st_elements('h3', 'left', '\n')
        values = get_values()
        selected_urls = select_urls(values, select_values(values))
        add_st_elements('h3', 'left', 'Choisissez un fichier à charger')
        uploaded_file = st.file_uploader('', type='csv', accept_multiple_files=False)
        add_st_elements('h3', 'left', '\n')
        if st.button('Suivant') or 'suivant' in st.session_state:
            if 'suivant' not in st.session_state:
                st.session_state['suivant'] = 'ok'
            if not uploaded_file:
                if selected_urls is not None:
                    async with aiohttp.ClientSession() as session:
                        responses = await asyncio.gather(*[get_datas(session, selected_url) for selected_url in selected_urls])
                    write_to_database(responses, 'download')
            else:
                write_to_database(uploaded_file, 'upload')
    if 'database' in st.session_state:
        add_st_elements('h3', 'left', 'Base de données initialisées')
        if st.button('Reset'):
            reset_database()
        if 'database' in st.session_state:
            add_st_elements('h3', 'left', '\n')
            add_st_elements('h3', 'left', 'Choisissez les contraintes que vous souhaitez')
            selected_constraints = select_constraints(get_columns())
            add_st_elements('h3', 'left', '\n')
            if st.button('Analyser'):
                start = time.time()
                result = analyse(selected_constraints)
                add_st_elements('h3', 'left', "Résultat de l'analyse")
                display_result(result, get_rows())
                add_st_elements('p', 'left', str("{:.2f}".format(time.time() - start)) + ' s pour analyser les données')


asyncio.run(streamlit_main())
