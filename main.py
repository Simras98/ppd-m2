from sqlalchemy import create_engine
from datetime import datetime
from bs4 import BeautifulSoup
import aiohttp as aiohttp
import streamlit as st
import pandas as pd
import numpy as np
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


def cast_to_dataframe(data, choice):
    if choice == 'download':
        temp = []
        for element in data:
            temp.append(pd.read_csv(element, index_col=None, header=0, dtype=str))
        dataframe = pd.concat(temp, axis=0, ignore_index=True)
    elif choice == 'upload':
        dataframe = pd.read_csv(data, index_col=None, header=0, dtype=str)
    return dataframe


def create_database(dataframe):
    db_connection = pymysql.connect(host='127.0.0.1', user='root', port=3306, password='password')
    try:
        with db_connection.cursor() as cursor:
            cursor.execute('CREATE DATABASE IF NOT EXISTS ppd')
    finally:
        db_connection.close()
    db_connection = create_engine('mysql+pymysql://root:password@127.0.0.1/ppd', pool_recycle=3600).connect()
    try:
        dataframe.to_sql('yellow_tripdata', db_connection, if_exists='replace')
        db_connection.close()
        return True
    except (Exception,):
        db_connection.close()
        return False


def get_constraints():
    return {
        'congestion_surcharge': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['', '']],
        'DOLocationID': [[pd.to_numeric, 'Est de type int'], [None, "N'est pas vide"], ['', '']],
        'extra': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], [['0.5', '1.0'], 'Appartient à [0.5, 1.0]']],
        'fare_amount': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['> 0', 'Est > 0']],
        'improvement_surcharge': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['', '']],
        'mta_tax': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['= 0.5', 'Est = 0.5']],
        'passenger_count': [[pd.to_numeric, 'Est de type int'], [None, "N'est pas vide"], ['>= 1', 'Est >= 1']],
        'payment_type': [[pd.to_numeric, 'Est de type int'], [None, "N'est pas vide"], [['1', '2', '3', '4', '5', '6'], 'Appartient à [1, 2, 3, 4, 5, 6]']],
        'PULocationID': [[pd.to_numeric, 'Est de type int'], [None, "N'est pas vide"], ['', '']],
        'RatecodeID': [[pd.to_numeric, 'Est de type int'], [None, "N'est pas vide"], [['1', '2', '3', '4', '5', '6'], 'Appartient à [1, 2, 3, 4, 5, 6]']],
        'store_and_fwd_flag': [[str, 'Est de type str'], [None, "N'est pas vide"], [['Y', 'N'], 'Appartient à [Y, N]']],
        'tip_amount': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['>= 0', 'Est >= 0']],
        'tolls_amount': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['>= 0', 'Est >= 0']],
        'total_amount': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['>= 0', 'Est >= 0']],
        'tpep_dropoff_datetime': [[pd.to_datetime, 'Est de type date'], [None, "N'est pas vide"], ['> tpep_pickup_datetime', 'Est > tpep_pickup_datetime']],
        'tpep_pickup_datetime': [[pd.to_datetime, 'Est de type date'], [None, "N'est pas vide"], ['< tpep_dropoff_datetime', 'Est < tpep_dropoff_datetime']],
        'trip_distance': [[pd.to_numeric, 'Est de type float'], [None, "N'est pas vide"], ['>= 0', 'Est >= 0']],
        'VendorID': [[pd.to_numeric, 'Est de type int'], [None, "N'est pas vide"], [['1', '2'], 'Appartient à [1, 2]']]}


def select_constraints(columns):
    constraints = get_constraints()
    selected_constraints = {}
    for column in columns:
        if st.checkbox(column, key=column):
            column_constraints = [element[1] for element in constraints[column]]
            for x, col in enumerate(st.columns(len(column_constraints))):
                if col.checkbox(str(column_constraints[x]), key=str(column) + ' ' + str(column_constraints[x])):
                    if column not in selected_constraints:
                        selected_constraints[column] = [constraints[column][x][0]]
                    else:
                        selected_constraints[column] = selected_constraints[column] + [constraints[column][x][0]]
    return selected_constraints


def get_result(column, column_two, contraint):
    if contraint == '':
        return 0
    if contraint == '< tpep_dropoff_datetime':
        return np.where((column < column_two), False, True).sum()
    if contraint == '> tpep_pickup_datetime':
        return np.where((column > column_two), False, True).sum()
    if contraint in [pd.to_numeric, pd.to_datetime]:
        return contraint(column, errors='coerce').isna().sum()
    if contraint in [str, None]:
        return column.isna().sum()
    if type(contraint) is list:
        return column[column.isin(contraint) == False].count()
    if contraint in ['>= 0', '>= 1', '> 0', '> 1', '= 0.5']:
        casted_column = pd.to_numeric(column, errors='coerce')
        casted_compared_value = np.float64(contraint.split()[1])
        comparator = contraint.split()[0]
        if comparator == '>=':
            return column[casted_column < casted_compared_value].count()
        if comparator == '>':
            return column[casted_column <= casted_compared_value].count()
        if comparator == '<':
            return column[casted_column >= casted_compared_value].count()
        if comparator == '=':
            return column[casted_column != casted_compared_value].count()


def analyse(dataframe, contraints):
    result = {}
    for column in contraints.keys():
        result[column] = [None] * 3
        for x, constraint in enumerate(contraints[column]):
            if column == 'tpep_pickup_datetime' and constraint == '< tpep_dropoff_datetime':
                result[column][x] = get_result(dataframe[column], dataframe['tpep_dropoff_datetime'], constraint)
            elif column == 'tpep_dropoff_datetime' and constraint == '> tpep_pickup_datetime':
                result[column][x] = get_result(dataframe[column], dataframe['tpep_pickup_datetime'], constraint)
            else:
                result[column][x] = get_result(dataframe[column], None, constraint)
    return result


def display_result(result, dataframe):
    constraints = get_constraints()
    for key, value in result.items():
        result[key] = [constraints[key][x][1] + ' : ' + str(int(100 - element / len(dataframe[key]))) + ' %' if element is not None else '' for x, element in enumerate(value)]
    st.markdown("""<style>.row_heading.level0 {display:none}.blank {display:none}</style>""", unsafe_allow_html=True)
    st.table(result)


async def streamlit_main():
    st.set_page_config(layout='centered')
    add_st_elements('h1', 'left', "Application d'évaluation de qualité")
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
                dataframe = cast_to_dataframe(responses, 'download')
        else:
            dataframe = cast_to_dataframe(uploaded_file, 'upload')
        if create_database(dataframe):
            add_st_elements('h3', 'left', 'Choisissez les contraintes que vous souhaitez')
            selected_constraints = select_constraints(dataframe.columns)
            add_st_elements('h3', 'left', '\n')
            if st.button('Analyser'):
                start = time.time()
                result = analyse(dataframe, selected_constraints)
                add_st_elements('h3', 'left', "Résultat de l'analyse")
                display_result(result, dataframe)
                add_st_elements('p', 'left', str("{:.2f}".format(time.time() - start)) + ' s pour analyser les données')


asyncio.run(streamlit_main())
