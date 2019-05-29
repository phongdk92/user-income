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
from datetime import datetime, timedelta

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

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

VIETNAM_REGIONS = "external_data/location/Vietnam regions.xlsx"


def load_data(path, filename, nrows=None):
    # be carefull with data, since it could have duplicate user_id in different day
    filepath = os.path.join(path, filename)
    print("Load data from file : {}".format(filepath))

    try:
        df = pd.read_csv(filepath, dtype={'user_id': str}, nrows=nrows)
    except:
        print("---------- Cannot load data from file : {} ---------------".format(filepath))
        df = []
    return df


def process_url_property(path, filename, property):
    name = property.get_name()
    threshold = property.get_threshold()
    print('------------------Process {} -----------------'.format(name.upper()))
    dfs = []
    for day in range(num_days):
        date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d")
        dfs.append(load_data(os.path.join(path, date), filename))
    df_property = pd.concat(dfs)
    print(df_property.shape)

    print('Filter with THRESHOLD : {}'.format(threshold))
    df = pd.DataFrame()
    df['user_id'] = df_property['user_id']
    print("number unique users : {}".format(len(df['user_id'].unique())))
    # filter all users (with duplicate) with count in a day >= threshold
    df['flag'] = df_property['count'].apply(lambda x: int(x >= threshold))

    # if all day a user read n_shopping pages < threshold -> not potential customer
    # it means sum of all day for that user = 0
    df = df.groupby('user_id').sum()
    df[name] = (df['flag'] > 0).astype(np.int8)
    print('Unique number of users in days is considered "active customer" : {}'.format(len(df[df['flag'] > 0])))
    print('Unique number of users in days is considered "non-active customer" : {}'.format(len(df[df['flag'] == 0])))

    #    df.drop(columns=['flag'], inplace=True)
    # df[name + "_score"] = df[name].apply(lambda x: property.get_score(x))
    df[name + "_score"] = df[name].map(property.get_map_score())
    return df[name + "_score"]


def process_demography(path, filename, list_properties, nrows=None):
    print("---------------------------PROCESS DEMOGRAPHY------------------")
    df = load_data(os.path.join(path, end_date), filename, nrows=nrows)
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
    # df = load_data(path, filename, nrows=nrows)
    dfs = []
    for day in range(num_days):
        date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d")
        dfs.append(load_data(os.path.join(path, date), filename))
    df = pd.concat(dfs)
    df = df.drop_duplicates()
    # https://stackoverflow.com/questions/27236275/what-does-valueerror-cannot-reindex-from-a-duplicate-axis-mean
    df.index = range(len(df))  # have to reindex 0...N

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
    df = df.groupby("user_id")[col_name].max()  # max score for all devices each user has (the most expensive device)
    return df


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
    # df = load_data(path, filename, nrows=nrows)
    dfs = []
    for day in range(num_days):
        date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d")
        dfs.append(load_data(os.path.join(path, date), filename))
    df = pd.concat(dfs)
    print("Before drop duplicates: {}".format(df.shape))
    df = df.drop_duplicates()
    print("After drop duplicates: {}".format(df.shape))

    df["province"] = df["location"].apply(lambda x: str(x)[1:4])  # get name province
    df = df.drop_duplicates(subset=["user_id", "province"], keep="first")
    # compute address_score for each address using a predefinded dict
    df["address"] = df["location"].map(code2address).map(address_map_score).fillna(address_map_score["others"])
    score = df.groupby('user_id').agg({'address': 'mean', 'province': 'size'})  # count how many provinces for each uid
    score.columns = ['address_score', 'num_travel']
    # compute travel_score for each uid using a predefinded dict
    score['travel_score'] = score['num_travel'].map(travel_map_score).fillna(travel_map_score["others"])
    print("Final shape : {}".format(score.shape))
    return score[["address_score", "travel_score"]]


def process_payment(path, filename):
    '''
    TODO: mask all users who already paid online as potential customers
    '''
    print("---------------------------PROCESS PAYMENT------------------")
    dfs = []
    for day in range(num_days):
        date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d")
        dfs.append(load_data(os.path.join(path, date), filename))
    df = pd.concat(dfs)
    print(df.shape)
    df = pd.DataFrame(df.groupby("user_id")['count'].sum())
    df['payment_score'] = 5.0
    print(df.shape)
    print(df.head())
    return df['payment_score']


def to_hour_distribution_df(dfs):
    '''
    :param dfs: user_id, hour, count
    :return: user_id, h_0, h_1, ... h_23 (distribution)
    '''
    dfs_by_uid = dfs.groupby(['user_id', 'hour']).sum()#.compute()
    dfs_unstack = dfs_by_uid.unstack(fill_value=0)
    dfs_results = dfs_unstack.reset_index()
    hour_columns = ['h_{}'.format(i) for i in range(24)] #list(np.arange(0,24))
    dfs_results.columns = ['user_id'] + hour_columns
    dfs_results[hour_columns] = dfs_results[hour_columns].div(dfs_results[hour_columns].sum(axis=1),
                                                              axis=0).astype(np.float16)
    dfs_results.set_index("user_id", inplace=True)
    return dfs_results


def process_daily_histogram(path, filename):
    print("---------------------------PROCESS DAILY HISTOGRAM------------------")
    WORK_STATION_THRESHOLD = 0.96  # np.percentile(df['working_proportion'], 70) ~ 96
    working_hours = ['h_{}'.format(i) for i in range(8, 20)]  # from 8am to 7pm
    dfs = []
    for day in range(num_days):
        date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d")
        dfs.append(load_data(os.path.join(path, date), filename))
    df = pd.concat(dfs)
    df = to_hour_distribution_df(df)
    df['working_proportion'] = df[working_hours].sum(axis=1)
    df['work_station'] = (df['working_proportion'] > WORK_STATION_THRESHOLD).astype(np.int8)
    print("Percentage of working station".format(df['work_station'].sum() * 100 / len(df)))
    return df['work_station']


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
    df_daily_histogram = process_daily_histogram(PATH, filename_daily_historgram)
    print(df_daily_histogram.head())
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start))
    start = time.time()
    df_payment = process_payment(PATH, filename_payment)
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start))
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

    start_time = time.time()
    df_total = merge_data(list_df)
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start_time))

    print(df_total.head())
    print(df_total.shape)
    df_total.to_csv('/home/phongdk/tmp/score_agg_daily.gz', compression='gzip', index=True)
    # # print("--- %s seconds ---" % (time.time() - start_time))
    # # print("Memory usage of properties dataframe is :", df_total.memory_usage().sum() / 1024 ** 2, " MB")
    #


if __name__ == '__main__':
    # from_date = '2019-05-11'
    # end_date = '2019-05-13'
    end_date = '2019-05-21'
    num_days = 1

    PATH = '/home/phongdk/data_user_income_targeting/data/'

    # have to put cluster, client inside main function
    cluster = LocalCluster(ip="0.0.0.0")
    client = Client(cluster)
    print(client)
    N_JOBS = 32

    if not os.path.exists(PATH):
        os.makedirs(PATH)

    filename_demography = "demography.gz"
    filename_hardware = "hardware.gz"
    filename_location = "location.gz"

    filename_airline = "airline.gz"
    filename_luxury = "luxury.gz"
    filename_booking_resort = "booking_resort.gz"
    filename_booking_hotel = "booking_hotel.gz"
    filename_tour = "tour.gz"
    filename_shopping = "shopping.gz"

    filename_payment = "payment.gz"
    filename_daily_historgram = "daily_histogram.gz"

    filename_based_url = [filename_airline, filename_luxury, filename_booking_resort, filename_booking_hotel,
                          filename_tour, filename_shopping]
    class_property_based_url = [Airline('airline'), Luxury('luxury'), Resort('resort'), Hotel('hotel'),
                                Tour('tour'), Shopping('shopping')]

    assert len(filename_based_url) == len(class_property_based_url)
    process()
