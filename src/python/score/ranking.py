#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 07 14:27 2018

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
#from os_name import OS_name
from device import Device

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


def load_data(filename, nrows=None):
    # be carefull with data, since it could have duplicate user_id in different day
    print("Load data from file : {}".format(filename))
    df = pd.read_csv(os.path.join(PATH, filename), dtype={'user_id': str}, nrows=nrows)
    return df


def process_url_property(df_property, count_name, threshold):
    print('------------------Process {}: {} -----------------'.format(count_name, df_property.shape))
    print('Filter with THRESHOLD : {}'.format(threshold))
    df = pd.DataFrame()
    df['user_id'] = df_property['user_id']
    print("number unique users : {}".format(len(df['user_id'].unique())))
    # filter all users (with duplicate) with count in a day >= threshold
    df['flag'] = df_property['count'].apply(lambda x: int(x >= threshold))

    # if all day a user read n_shopping pages < threshold -> not potential customer
    # it means sum of all day for that user = 0
    df = df.groupby('user_id').sum()
    df[count_name] = df['flag'].apply(lambda x: int(x > 0))
    print('Unique number of users in days is considered "active customer" : {}'.format(len(df[df['flag'] > 0])))
    print('Unique number of users in days is considered "non-active customer" : {}'.format(len(df[df['flag'] == 0])))

    df.drop(columns=['flag'], inplace=True)
    return df


def process_demography(df_demography):
    print("---------------------------PROCESS DEMOGRAPHY------------------")
    df = df_demography[['user_id', 'gender', 'age']]
    df.set_index('user_id', inplace=True)
    return df


def process_hardware_location(df_hardware_location):
    print("---------------------------PROCESS HARDWARE------------------")
    # df = df_hardware_location[['user_id', 'os_name']]
    df = df_hardware_location[['user_id', 'os_name', 'hw_class', 'cpu', 'sys_ram_mb', 'screen_height', 'screen_width']]
    df.set_index('user_id', inplace=True)
    return df


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
    df_total.fillna(-1, inplace=True)
    for col in df_total.columns:
        try:
            df_total[col] = df_total[col].astype(int)
        except:
            pass
    return df_total


def compute_score(df, properties):
    print('----------------------Compute SCORE ---------------------------')
    # df['score'] = np.zeros(len(df))
    # for col in df.columns:
    #     df['score'] = df.apply(lambda x: x['score'] + map_score[col][x[col]])
    for (i, proper) in enumerate(properties):  # can be paralleled
        col = proper.get_name()
        print(col)
        if col == 'device':
            df[col + "_score"] = df[['os_name', 'hw_class', 'cpu', 'sys_ram_mb',
                                     'screen_height', 'screen_width']].apply(lambda x: proper.get_score(x), axis=1)
        else:
            df[col + "_score"] = df[col].apply(lambda x: proper.get_score(x))
    col_score = [col for col in df.columns if 'score' in col]
    df = df[col_score]
    df['total_score'] = df.sum(axis=1)
    print("Memory usage of properties dataframe is :", df.memory_usage().sum() / 1024 ** 2, " MB")
    df.to_csv('/home/phongdk/tmp/score2.csv.gz', compression='gzip', index=True)
    print(df.head())


# def merge_data(list_df):
#     print("-----------------------Merge data-----------------------------")
#     list_dask_df = [dd.from_pandas(df, npartitions=4) for df in list_df]  # convert to Dask DF with chunksize 64MB
#
#     for (i, df) in enumerate(list_dask_df):
#         if i == 0:
#             df_total = df[:]
#         else:
#             df_total = df_total.merge(df, left_index=True, right_index=True, how='outer')
#     print(type(df_total))
#     df_total = df_total.fillna(-1)
#     str_columns = ['os_name']
#     for col in df_total.columns:
#         if col not in str_columns:
#             df_total[col] = df_total[col].astype(int)
#     #print('Convert back to pandas')
#     #df_total = df_total.compute()
#     #print(type(df_total))
#     return df_total
#
#
# def compute_score(df, properties):
#     print('----------------------Compute SCORE ---------------------------')
#     def get_score_dask(df, col):
#         df[col]
#         return
#     # df['score'] = np.zeros(len(df))
#     # for col in df.columns:
#     #     df['score'] = df.apply(lambda x: x['score'] + map_score[col][x[col]])
#     #total_score = np.zeros(len(df))
#     for (i, proper) in enumerate(properties):
#         col = proper.get_name()
#         #df[col + "_score"] = df[col].apply(lambda x: proper.get_score(x))
#         #df.apply(lambda x: proper.get_score(x[col]))
#         #df[col + "_score"] = df.apply(lambda x: proper.get_score(x[col]), axis=1)
#         #df[col + "_score"] = df[col].map_partitions(proper.get_score, meta={col: 'i8'})
#         name = col + "_score"
#         df = df.map_partitions(lambda x: x.assign(name=proper.get_score(df[col])), meta={col: 'i8'})
#         #total_score += df[col + '_score'].values
#     print(df.head())
#     print("Memory usage of properties dataframe is :", df.memory_usage().sum() / 1024 ** 2, " MB")
#     col_score = [col for col in df.columns if 'score' in col]
#     df = df[col_score]
#     df['total_score'] = df.sum(axis=1).compute()
#     df.to_csv('/home/phongdk/tmp/score2.csv.gz', compression='gzip', index=True)
#     print(df.head())


def process():
    df_demography = load_data(filename_demography, nrows=1000)
    df_hardware_location = load_data(filename_hardware_location, nrows=1000)
    df_url_property = [load_data(filename) for filename in filename_based_url]

    # get_unique_user_id([df_demography, df_hardware_location, df_shopping, df_booking])
    # unique_users = get_unique_user_id([df_shopping, df_booking])
    # compute_score(unique_users)
    df_demography = process_demography(df_demography)
    df_hardware_location = process_hardware_location(df_hardware_location)

    list_df = [df_demography, df_hardware_location]
    for (df, class_property) in zip(df_url_property, class_property_based_url):
        list_df.append(process_url_property(df, count_name=class_property.get_name(),
                                            threshold=class_property.get_threshold()))

    start_time = time.time()
    df_total = merge_data(list_df)
    print("--- %s seconds ---" % (time.time() - start_time))
    print(df_total.head())
    del list_df
    gc.collect()
    print(df_total.shape)
    print("Memory usage of properties dataframe is :", df_total.memory_usage().sum() / 1024 ** 2, " MB")

    # properties = [Gender('gender'), Age('age'), OS_name('os_name')] + class_property_based_url
    properties = [Gender('gender'), Age('age'), Device('device')] + class_property_based_url
    assert(len(list_df) == len(properties))
    print(len(properties))
    compute_score(df_total, properties)
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    # have to put cluster, client inside main function
    # cluster = LocalCluster(ip="0.0.0.0")
    # client = Client(cluster)
    # print(client)

    from_date = '2018-11-14'
    end_date = '2018-11-27'
    PATH = '/home/phongdk/data_user_income_targeting'
    if not os.path.exists(PATH):
        os.makedirs(PATH)

    filename_demography = "demography_from_{}_to_{}.csv.gz".format(from_date, end_date)
    filename_hardware_location = "hardware_lat_lon_from_{}_to_{}.csv.gz".format(from_date, end_date)

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
