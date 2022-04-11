import numpy as np
from bs4 import BeautifulSoup
from calendar import month_name
import aiohttp as aiohttp
import pandas as pd
import streamlit as st
import asyncio
import io
import requests

def add_st_elements(type, text):
    st.markdown("<" + type + " style='text-align: center; color: white;'>" + text + "</" + type + ">", unsafe_allow_html=True)


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
            available_months = sorted(sorted(list(set([value[1] for value in values]))), key=list(month_name).index)
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


def get_constraints():
    return {
        'VendorID': [np.int64, None, ['1', '2']],
        'tpep_pickup_datetime': [np.datetime64, None, '< tpep_dropoff_datetime'],
        'tpep_dropoff_datetime': [np.datetime64, None, '> tpep_pickup_datetime'],
        'passenger_count': [np.int64, None, '> 1'],
        'trip_distance': [np.float64, None, '>= 0'],
        'RatecodeID': [np.int64, None, ['1', '2', '3', '4', '5', '6']],
        'store_and_fwd_flag': [str, None, ['Y', 'N']],
        'PULocationID': [np.int64, None, ''],
        'DOLocationID': [np.int64, None, ''],
        'payment_type': [np.int64, None, ['1', '2', '3', '4', '5', '6']],
        'fare_amount': [np.float64, None, '> 0'],
        'extra': [np.float64, None, ['0.5', '1']],
        'mta_tax': [np.float64, None, 0.5],
        'tip_amount': [np.float64, None, '>= 0'],
        'tolls_amount': [np.float64, None, '>= 0'],
        'improvement_surcharge': [np.float64, None, ''],
        'total_amount': [np.float64, None, '>= 0'],
        'congestion_surcharge': [np.float64, None, '']
    }


def select_constraints(columns, constraints):
    selected_constraints = []
    for column in columns:
        if st.checkbox(column, key=column):
            column_constraints = constraints[column]
            for x, col in enumerate(st.columns(len(column_constraints))):
                if col.checkbox(column_constraints[x], key=str(column) + ' ' + column_constraints[x]):
                    selected_constraints.append([column, column_constraints[x]])
    return selected_constraints


def get_result(value, selected_contraint):
    if selected_contraint in ['< tpep_dropoff_datetime', '> tpep_pickup_datetime', '']:
        return 0
    if selected_contraint in [np.int64, np.float64, np.datetime64, str]:
        try:
            selected_contraint(value)
            return 0
        except (Exception,):
            return 1
    if type(selected_contraint) is float:
        return 1 if not np.float64(value) == selected_contraint else 0
    if selected_contraint is None:
        return 1 if value is None else 0
    if type(selected_contraint) is list:
        return 1 if value not in selected_contraint else 0
    if selected_contraint in ['>= 0', '> 1', '> 0']:
        try:
            casted_value = np.float64(value)
            casted_compared_value = np.float64(selected_contraint.split()[1])
            comparator = selected_contraint.split()[0]
            if comparator == '>=':
                return 1 if not casted_value >= casted_compared_value else 0
            if comparator == '>':
                return 1 if not casted_value > casted_compared_value else 0
            if comparator == '<':
                return 1 if not casted_value < casted_compared_value else 0
        except (Exception,):
            return 1


def analyse(dataframe, constraints):
    result = {}
    for column in dataframe.columns:
        result[column] = [None] * len(constraints[column])
        for x, constraint in enumerate(constraints[column]):
            temp = 0
            for index, row in dataframe.iterrows():
                temp += get_result(row[column], constraint)
            result[column][x] = temp
    return result

async def streamlit_main():
    st.set_page_config(layout='centered')
    add_st_elements('h1', "Application d'évaluation de qualité")
    add_st_elements('h3', 'Filtrez les données que vous souhaitez télécharger')
    add_st_elements('h3', '\n')
    values = get_values()
    selected_urls = select_urls(values, select_values(values))
    add_st_elements('h3', 'Choisissez un fichier à charger')
    uploaded_file = st.file_uploader('', type='csv', accept_multiple_files=False)
    if st.button('Suivant'):
        if not uploaded_file:
            if selected_urls is not None:
                async with aiohttp.ClientSession() as session:
                    responses = await asyncio.gather(*[get_datas(session, selected_url) for selected_url in selected_urls])
                dataframe = cast_to_dataframe(responses, 'download')
        elif uploaded_file:
            dataframe = cast_to_dataframe(uploaded_file, 'upload')
        selected_constraints = select_constraints(dataframe.columns, get_constraints())
        if st.button('Analyser'):
            result = analyse(dataframe, get_constraints())
            st.dataframe(result)


asyncio.run(streamlit_main())