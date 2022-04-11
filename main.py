import numpy as np
from bs4 import BeautifulSoup
from calendar import month_name
import aiohttp as aiohttp
import pandas as pd
import streamlit as st
import asyncio
import io
import requests


def retrieve_values():
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


def fusion_datas(lists, choice):
    if choice == 'upload':
        li = []
        for element in lists:
            li.append(pd.read_csv(element, index_col=None, header=0, dtype=str))
        dataframe = pd.concat(li, axis=0, ignore_index=True)
    elif choice == 'file':
        dataframe = pd.read_csv(lists, index_col=None, header=0, dtype=str)
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
        'mta_tax': [np.float64, None, '0.5'],
        'tip_amount': [np.float64, None, '>= 0'],
        'tolls_amount': [np.float64, None, '>= 0'],
        'improvement_surcharge': [np.float64, None, ''],
        'total_amount': [np.float64, None, '>= 0'],
        'congestion_surcharge': [np.float64, None, '']
    }


def get_result(value, selected_contraint):
    if selected_contraint in ['< tpep_dropoff_datetime', '> tpep_pickup_datetime', '', '0.5']:
        return 0
    if selected_contraint in [np.int64, np.float64, np.datetime64, str]:
        try:
            selected_contraint(value)
            return 0
        except (Exception,):
            return 1
    if selected_contraint is None:
        return 1 if value is None else 0
    if type(selected_contraint) is list:
        return 1 if value not in selected_contraint else 0
    if selected_contraint in ['>= 0', '> 1', '> 0']:
        try:
            casted_value = np.float64(value)
            if selected_contraint.split()[0] == '>=':
                return 1 if not casted_value >= selected_contraint.split()[1] else 0
            if selected_contraint.split()[0] == '>':
                return 1 if not casted_value > selected_contraint.split()[1] else 0
            if selected_contraint.split()[0] == '<':
                return 1 if not casted_value < selected_contraint.split()[1] else 0
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
            print(column + ' ' + str(result[column][x]))


async def streamlit_main():
    values = retrieve_values()
    st.set_page_config(layout='centered')
    st.markdown("<h1 style='text-align: center; color: white;'>Application d'évaluation de qualité</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='color: white;'>Filtrez vos données à télécharger\n</h3>", unsafe_allow_html=True)
    st.markdown("<h3 style='color: white;'>\n</h3>", unsafe_allow_html=True)
    selected_files = []
    for avaialable_year in sorted(list(set([value[0] for value in values]))):
        if st.checkbox(avaialable_year, key=avaialable_year):
            available_months = sorted(sorted(list(set([value[1] for value in values]))), key=list(month_name).index)
            for x, col in enumerate(st.columns(len(available_months))):
                if col.checkbox(available_months[x], key=str(avaialable_year) + ' ' + available_months[x]):
                    selected_files.append([avaialable_year, available_months[x]])
    st.markdown("<h3 style='color: white;'>Choisissez un fichier à charger</h3>", unsafe_allow_html=True)
    uploaded_file = st.file_uploader('', type='csv', accept_multiple_files=False)
    if st.button('Suivant', key='Suivant1'):
        if not uploaded_file:
            selected_urls = select_urls(values, selected_files)
            if selected_urls is not None:
                async with aiohttp.ClientSession() as session:
                    responses = await asyncio.gather(*[get_datas(session, selected_url) for selected_url in selected_urls])
                dataframe = fusion_datas(responses, 'upload')
        elif uploaded_file:
            dataframe = fusion_datas(uploaded_file, 'file')
            analyse(dataframe, get_constraints())
        # rules = ['1 - Verifier son type', '2 - Verifier si vide', '3 - Verifier si nulle', '4 - Verifier sa règle de gestion']
        # selected_rules = []
        # for column in dataframe.columns:
        #     if st.checkbox(str(column), key=str(column)):
        #         available_filters = [rule[1] for rule in rules]
        #         for x, col in enumerate(st.columns(len(available_filters))):
        #             if col.checkbox(available_filters[x], key=str(column) + ' ' + available_filters[x]):
        #                 selected_rules.append([column, available_filters[x]])
        # if st.button('Suivant', key='Suivant2'):
        #     for value in selected_rules:
        #         print(value)


asyncio.run(streamlit_main())

# async def main():
#     urls = retrieve_urls()
#     initialize_streamlit(urls)
#     # # selected_urls = select_urls(urls)
#     # selected_urls = ['https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv', 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv']
#     # async with aiohttp.ClientSession() as session:
#     #     responses = await asyncio.gather(*[get_datas(session, selected_url) for selected_url in selected_urls])
#     # dataframe = fusion_datas(responses)


# def select_urls(urls):
#     choice = input('1 pour tout charger, 2 pour choisir : ')
#     if choice == '1':
#         return urls
#     if choice == '2':
#         avaialable_years = sorted(list(set([item[0] for item in urls])))
#         print(*avaialable_years, sep=', ')
#         years = input('Choisissez vos années en séparant par un , : ').split(',')
#         cleaned_years = [item for item in years if item in avaialable_years]
#         if len(years) != len(cleaned_years):
#             print('Les années selectionnées ne sont pas toutes correctes, un nettoyage a été effectué : ')
#             print(*cleaned_years, sep=', ')
#         avaialable_months = sorted(sorted(list(set([item[1] for item in urls]))), key=list(month_name).index)
#         print(*avaialable_months, sep=', ')
#         months = input('Choisissez vos mois en séparant par un , : ').split(',')
#         cleaned_months = [item for item in months if item in avaialable_months]
#         if len(months) != len(cleaned_months):
#             print('Les mois selectionnés ne sont pas toutes corrects, un nettoyage a été effectué : ')
#             print(*cleaned_months, sep=', ')
#         cleaned_years_urls = [url for url in urls if url[0] in cleaned_years]
#         return [url for url in cleaned_years_urls if url[1] in cleaned_months]
#     else:
#         print('Choix inconnu')
#         return None
