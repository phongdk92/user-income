#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 27 16:01 2019

@author: phongdk
"""

"""
TODO:
1. get all unique users from various sources
2. Compute score base on its properties like gender, age, hardware, or so 
"""

import os
import gc
import time
import numpy as np
import pandas as pd
import sys

sys.path.append('src/python/property')
sys.path.append('src/python/administrativeArea')

import warnings

warnings.filterwarnings("ignore")

# from score import map_score
from airline import Airline
from hotel import Hotel
from luxury import Luxury
from tour import Tour
from shopping import Shopping
from resort import Resort
from gender import Gender
from age import Age
# from os_name import OS_name
from device import Device
from address import Address
from vietnamADM import AdministrativeArea_KDTree

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


VIETNAM_REGIONS = "external_data/location/Vietnam regions.xlsx"


def load_data(path, filename, nrows=None):
    # be carefull with data, since it could have duplicate user_id in different day
    print("Load data from file : {}".format(filename))
    df = pd.read_csv(os.path.join(path, filename), dtype={'user_id': str}, nrows=nrows)
    return df


def process_url_property(path, filename, property):
    name = property.get_name()
    threshold = property.get_threshold()
    print('------------------Process {} -----------------'.format(name.upper()))
    df_property = load_data(path, filename)

    print('Filter with THRESHOLD : {}'.format(threshold))
    df = pd.DataFrame()
    df['user_id'] = df_property['user_id']
    print("number unique users : {}".format(len(df['user_id'].unique())))
    # filter all users (with duplicate) with count in a day >= threshold
    df['flag'] = df_property['count'].apply(lambda x: int(x >= threshold))

    # if all day a user read n_shopping pages < threshold -> not potential customer
    # it means sum of all day for that user = 0
    df = df.groupby('user_id').sum()
    df[name] = df['flag'].apply(lambda x: int(x > 0))
    print('Unique number of users in days is considered "active customer" : {}'.format(len(df[df['flag'] > 0])))
    print('Unique number of users in days is considered "non-active customer" : {}'.format(len(df[df['flag'] == 0])))

    #    df.drop(columns=['flag'], inplace=True)
    df[name + "_score"] = df[name].apply(lambda x: property.get_score(x))
    return df[name + "_score"]


def process_demography(path, filename, list_properties, nrows=None):
    print("---------------------------PROCESS DEMOGRAPHY------------------")
    df = load_data(path, filename, nrows=nrows)
    # df = df[['user_id', 'gender', 'age']]
    for col in ['gender', 'age']:
        df[col] = df[col].astype(np.int8)
    columns = []
    for property in list_properties:
        col_name = f"{property.get_name()}_score"
        columns.append(col_name)
        df[col_name] = df[property.get_name()].apply(lambda x: property.get_score(x))
    df.set_index('user_id', inplace=True)
    return df[columns]


def process_hardware(path, filename, property, nrows=None):
    print("---------------------------PROCESS HARDWARE------------------")  # convert to DASK for parallel processing
    df = load_data(path, filename, nrows=nrows)
    # df = df[['user_id', 'os_name', 'hw_class', 'cpu', 'sys_ram_mb', 'screen_height', 'screen_width']]
    for col in ['sys_ram_mb', 'screen_height', 'screen_width']:
        df[col] = df[col].fillna(-1).astype(np.int8)
    for col in ['cpu']:
        df[col] = df[col].fillna(-1)
    print('Get score device')
    print(df.shape)
    df_dask = dd.from_pandas(df, npartitions=N_JOBS)
    score_device = df_dask[['os_name', 'hw_class', 'cpu', 'sys_ram_mb', 'screen_height',
                            'screen_width']].apply(lambda x: property.get_score(x), axis=1).compute()
    col_name = f"{property.get_name()}_score"
    df[col_name] = score_device
    df.set_index('user_id', inplace=True)

    # df[col_name] = df[['os_name', 'hw_class', 'cpu', 'sys_ram_mb', 'screen_height',
    #                    'screen_width']].apply(lambda x: property.get_score(x), axis=1)
    return df[col_name]


def get_code2address(filename="external_data/location/Vietnam regions.xlsx"):
    CITY = ["hà nội", "hồ chí minh", "đà nẵng", "hải phòng", "cần thơ"]
    df = pd.read_excel(filename, sheet_name='Districts')
    df.columns = ["district_id", "district_name", "province_id", "province_name"]
    df.set_index("district_id", inplace=True)
    df["district_name"] = df["district_name"].apply(lambda x: x.lower().strip())
    df["province_name"] = df["province_name"].apply(lambda x: x.lower().strip())
    df["address"] = df.apply(lambda x: x["province_name"] if x["province_name"] in CITY else x["district_name"], axis=1)
    new_dict = df["address"].to_dict()
    return new_dict


def process_location(path, filename, property, nrows=None):
    print("---------------------------PROCESS LOCATION------------------")
    code2address = get_code2address(filename=VIETNAM_REGIONS)
    address_map_score = property.get_address_map_score()
    travel_map_score = property.get_travel_map_score()
    df = load_data(path, filename, nrows=nrows)
    df["province"] = df["location"].apply(lambda x: str(x)[1:4]) # get name province
    df = df.drop_duplicates(subset=["user_id", "province"], keep="first")
    # compute address_score for each address using a predefinded dict
    df["address"] = df["location"].map(code2address).map(address_map_score).fillna(address_map_score["others"])
    score = df.groupby('user_id').agg({'address': 'mean', 'province': 'size'})  # count how many provinces for each uid
    score.columns = ['address_score', 'num_travel']
    # compute travel_score for each uid using a predefinded dict
    score['travel_score'] = score['num_travel'].map(travel_map_score).fillna(travel_map_score["others"])
    return score[["address_score", "travel_score"]]


def get_unique_user_id(list_df):
    list_index = []
    for x in list_df:
        list_index.extend(list(x.index))

    print("Number of unique users (with duplicates) : {}".format(len(list_index)))
    unique_users = sorted(set(list_index))
    print("Number of unique users : {}".format(len(unique_users)))
    return unique_users


def merge_data(list_df):
    print("-----------------------Merge data-----------------------------")
    for (i, df) in enumerate(list_df):
        if i == 0:
            df_total = df[:]
        else:
            df_total = pd.merge(df_total, df, left_index=True, right_index=True, how='outer')
    df_total.fillna(0, inplace=True)
    return df_total


def process():
    nrows = None
    start = time.time()
    df_hardware = process_hardware(PATH, filename_hardware, Device('device'), nrows=nrows)
    print(df_hardware.head())
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start))
    start = time.time()
    df_demography = process_demography(PATH, filename_demography, [Gender('gender'), Age('age')], nrows=nrows)
    print(df_demography.head())
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start))
    start = time.time()
    df_location = process_location(PATH, filename_location, Address('address'), nrows=nrows)
    print(df_location.head())
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start))

    # start = time.time()
    list_df = [df_demography, df_hardware, df_location]
    for (filename, class_property) in zip(filename_based_url, class_property_based_url):
        list_df.append(process_url_property(PATH, filename, class_property))

    # properties = [Gender('gender'), Age('age')] + class_property_based_url + [Device('device')]
    # print(len(list_df), len(properties))
    start_time = time.time()
    df_total = merge_data(list_df)
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start_time))

    print(df_total.head())
    print(df_total.shape)
    df_total.to_csv('/home/phongdk/tmp/score.gz', compression='gzip', index=True)
    # # print("--- %s seconds ---" % (time.time() - start_time))
    # # print("Memory usage of properties dataframe is :", df_total.memory_usage().sum() / 1024 ** 2, " MB")
    #
    # # properties = [Gender('gender'), Age('age'), OS_name('os_name')] + class_property_based_url
    # compute_score(df_total, properties)
    # print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    from_date = '2019-04-30'
    end_date = '2019-05-13'

    PATH = '/home/phongdk/data_user_income_targeting/2019-05-13'

    # have to put cluster, client inside main function
    cluster = LocalCluster(ip="0.0.0.0")
    client = Client(cluster)
    print(client)
    N_JOBS = 32

    if not os.path.exists(PATH):
        os.makedirs(PATH)

    filename_demography = "demography_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_hardware = "hardware_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_location = "location_from_{}_to_{}.csv.gz".format(from_date, end_date)

    filename_airline = "airline_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_luxury = "luxury_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_booking_resort = "booking_resort_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_booking_hotel = "booking_hotel_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_tour = "tour_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_shopping = "shopping_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_based_url = [filename_airline, filename_luxury, filename_booking_resort, filename_booking_hotel,
                          filename_tour, filename_shopping]
    class_property_based_url = [Airline('airline'), Luxury('luxury'), Resort('resort'), Hotel('hotel'),
                                Tour('tour'), Shopping('shopping')]

    assert len(filename_based_url) == len(class_property_based_url)
    process()
