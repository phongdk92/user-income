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
import argparse
import logging
from datetime import datetime, timedelta
from multiprocessing import Pool
sys.path.append('src/property')
sys.path.append('src/db')

import warnings

warnings.filterwarnings("ignore")

from airline import Airline
from hotel import Hotel
from luxury import Luxury
from tour import Tour
from shopping import Shopping
from resort import Resort
from gender import Gender
from age import Age
from device import Device
from address import Address

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

from incomeClassification import IncomeClassification
from redisConnection import RedisConnection
from utils import load_multiple_data, classify, get_milestone, load_data, to_hour_distribution_df, get_code2address

VIETNAM_REGIONS = "external_data/location/Vietnam regions.xlsx"
LOG_DIR = "logs"

LUXURY_PROP = 92
HIGH_PROP = 80
MIDDLE_PROP = 35
UPPER_OUTLIER = 99.5


def convert(x):     # have to put redisConn.get_browser_id into a function to parallel
    return redisConn.get_browser_id(x)


def convert_hashID_to_browser_id(df):
    LOGGER.info('-------------Convert hashID to Browser_ID---------------')
    LOGGER.info("Shape before convert HashId {}".format(df.shape))
    if 'user_id' in df.columns:
        # df["user_id"] = df["user_id"].apply(lambda x: redisConn.get_browser_id(x))
        df["user_id"] = pool.map(convert, df["user_id"])
        df.dropna(subset=["user_id"], inplace=True)
    else:
        df.index = pool.map(convert, df.index)
        df = df[df.index.notnull()]
        df.index.name = "user_id"
    LOGGER.info("Shape before convert HashId {}".format(df.shape))
    return df


def process_url_property(path, filename, property):
    name = property.get_name()
    LOGGER.info('------------------Process {} -----------------'.format(name.upper()))
    print('------------------Process {} -----------------'.format(name.upper()))

    df = load_multiple_data(path, filename, END_DATE=END_DATE, num_day_back=NUM_DAYS_BACK)
    df = pd.DataFrame(df.groupby('user_id')['count'].agg('sum'))
    milestones, upper_outlier = get_milestone(df['count'].values, luxury=LUXURY_PROP, high=HIGH_PROP,
                                              middle=MIDDLE_PROP, upper_outlier=UPPER_OUTLIER)
    print(milestones, upper_outlier)
    df[name] = df['count'].apply(lambda x: classify(x, milestones))
    df[df['count'] >= upper_outlier][name] = 0        # remove upper outlier users
    df[name + "_score"] = df[name].map(property.get_map_score())
    LOGGER.info(df.head())
    return df[name + "_score"]


def process_demography(path, filename, list_properties, nrows=None):
    LOGGER.info("---------------------------PROCESS DEMOGRAPHY------------------")
    df = load_data(os.path.join(path, END_DATE), filename, nrows=nrows)
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
    LOGGER.info("---------------------------PROCESS HARDWARE------------------")
    # df = load_data(path, filename, nrows=nrows)
    df = load_multiple_data(path, filename, END_DATE=END_DATE, num_day_back=NUM_DAYS_BACK)
    df = df.drop_duplicates()
    # https://stackoverflow.com/questions/27236275/what-does-valueerror-cannot-reindex-from-a-duplicate-axis-mean
    df.index = range(len(df))  # have to reindex 0...N

    for col in ['sys_ram_mb', 'screen_height', 'screen_width']:
        df[col] = df[col].fillna(-1).astype(np.int8)
    for col in ['cpu']:
        df[col] = df[col].fillna(-1)
    LOGGER.info('Get score device')
    LOGGER.info(df.shape)
    df_dask = dd.from_pandas(df, npartitions=N_JOBS)    # convert to DASK for parallel processing
    score_device = df_dask[['os_name', 'hw_class', 'cpu', 'sys_ram_mb', 'screen_height',
                            'screen_width']].apply(lambda x: property.get_score(x), axis=1).compute()
    col_name = f"{property.get_name()}_score"
    df[col_name] = score_device
    df = df.groupby("user_id")[col_name].max()  # max score for all devices each user has (the most expensive device)
    return df


def process_location(path, filename, property, nrows=None):
    LOGGER.info("---------------------------PROCESS LOCATION------------------")
    code2address = get_code2address(filename=VIETNAM_REGIONS)
    address_map_score = property.get_address_map_score()
    travel_map_score = property.get_travel_map_score()

    df = load_multiple_data(path, filename, END_DATE=END_DATE, num_day_back=NUM_DAYS_BACK)
    LOGGER.info("Before drop duplicates: {}".format(df.shape))
    df = df.drop_duplicates()
    LOGGER.info("After drop duplicates: {}".format(df.shape))

    df["province"] = df["location"].apply(lambda x: str(x)[1:4])  # get name province
    df = df.drop_duplicates(subset=["user_id", "province"], keep="first")
    # compute address_score for each address using a predefinded dict
    df["address"] = df["location"].map(code2address).map(address_map_score).fillna(address_map_score["others"])
    score = df.groupby('user_id').agg({'address': 'mean', 'province': 'size'})  # count how many provinces for each uid
    score.columns = ['address_score', 'num_travel']
    # compute travel_score for each uid using a predefinded dict
    score['travel_score'] = score['num_travel'].map(travel_map_score).fillna(travel_map_score["others"])
    LOGGER.info("Final shape : {}".format(score.shape))
    return score[["address_score", "travel_score"]]


def process_payment(path, filename):
    '''
    TODO: mask all users who already paid online as potential customers
    '''
    LOGGER.info("---------------------------PROCESS PAYMENT------------------")
    SCORE_PAYMENT = 5.0
    df = load_multiple_data(path, filename, END_DATE=END_DATE, num_day_back=NUM_DAYS_BACK)
    LOGGER.info(df.shape)
    df = pd.DataFrame(df.groupby("user_id")['count'].sum())
    df['payment_score'] = SCORE_PAYMENT
    return df['payment_score']


def process_daily_histogram(path, filename):
    LOGGER.info("---------------------------PROCESS DAILY HISTOGRAM------------------")
    WORK_STATION_THRESHOLD = 0.96  # np.percentile(df['working_proportion'], 70) ~ 96
    working_hours = ['h_{}'.format(i) for i in range(8, 20)]  # from 8am to 7pm
    df = load_multiple_data(path, filename, END_DATE=END_DATE, num_day_back=NUM_DAYS_BACK)
    df = to_hour_distribution_df(df)
    df['working_proportion'] = df[working_hours].sum(axis=1)
    df['work_station'] = (df['working_proportion'] > WORK_STATION_THRESHOLD).astype(np.int8)
    LOGGER.info("Percentage of working station".format(df['work_station'].sum() * 100 / len(df)))
    return df['work_station']


def merge_data(list_df):
    LOGGER.info("-----------------------Merge data-----------------------------")
    for (i, df) in enumerate(list_df):
        if i == 0:
            df_total = df[:]
        else:
            df_total = pd.merge(df_total, df, left_index=True, right_index=True, how='outer')
    df_total.fillna(0, inplace=True)
    return df_total


def process():
    nrows = None
    try:
        start = time.time()
        df_daily_histogram = process_daily_histogram(DATA_PATH, filename_daily_historgram)
        LOGGER.info(df_daily_histogram.head())
        print("Time ------------------df_daily_histogram------------------------- {:.2f} (s)".format(time.time() - start))
    except:
        df_daily_histogram = pd.DataFrame()
    try:
        start = time.time()
        df_payment = process_payment(DATA_PATH, filename_payment)
        LOGGER.info(df_payment.head())
        print("Time ------------------df_payment------------------------- {:.2f} (s)".format(time.time() - start))
    except:
        df_payment = pd.DataFrame()
    try:
        start = time.time()
        df_hardware = process_hardware(DATA_PATH, filename_hardware, Device('device'), nrows=nrows)
        LOGGER.info(df_hardware.head())
        print("Time -------------------df_hardware------------------------ {:.2f} (s)".format(time.time() - start))
    except:
        df_hardware = pd.DataFrame()

    try:
        start = time.time()
        df_demography = process_demography(DATA_PATH, filename_demography, [Gender('gender'), Age('age')], nrows=nrows)
        LOGGER.info(df_demography.head())
        print("Time -------------------df_demography------------------------ {:.2f} (s)".format(time.time() - start))
    except:
        df_demography = pd.DataFrame()
    try:
        start = time.time()
        df_location = process_location(DATA_PATH, filename_location, Address('address'), nrows=nrows)
        LOGGER.info(df_location.head())
        print("Time --------------------df_location----------------------- {:.2f} (s)".format(time.time() - start))
    except:
        df_location = pd.DataFrame()
    # start = time.time()
    list_df = [df_daily_histogram, df_payment, df_demography, df_hardware, df_location]
    for (filename, class_property) in zip(filename_based_url, class_property_based_url):
        list_df.append(process_url_property(DATA_PATH, filename, class_property))

    start_time = time.time()
    df_total = merge_data(list_df)
    print("Time --------------------merge_data----------------------- {:.2f} (s)".format(time.time() - start_time))
    LOGGER.info(df_total.head())
    LOGGER.info(df_total.shape)

    income_classification = IncomeClassification(luxury=LUXURY_PROP, high=HIGH_PROP, middle=MIDDLE_PROP)
    df_total = income_classification.classify_income(df_total)
    # df_total = df_total[df_total['income'] > 0]     # remove low income users to save storage
    df_total = convert_hashID_to_browser_id(df_total)
    df_total['income'].to_csv(OUTPUT_FILENAME, compression='gzip', index=True)
    df_total.to_csv('/home/phongdk/data_user_income_targeting/score/score.gz', compression='gzip', index=True)
    # LOGGER.info("--- %s seconds ---" % (time.time() - start_time))
    # LOGGER.info("Memory usage of properties dataframe is :", df_total.memory_usage().sum() / 1024 ** 2, " MB")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--date", required=True, help="date to run")
    ap.add_argument("-p", "--data_path", required=True, help="path to input folder")
    ap.add_argument("-o", "--output", required=True, help="path to output file")
    ap.add_argument("-b", "--back_date", required=False, nargs='?', help="path to model directory", const=True,
                    type=int, default=1)
    ap.add_argument("-n", "--n_jobs", required=False, nargs='?', help="number of jobs to parallel", const=True,
                    type=int, default=24)

    args = vars(ap.parse_args())
    END_DATE = args['date']
    DATA_PATH = args['data_path']
    OUTPUT_FILENAME = args['output']
    NUM_DAYS_BACK = args['back_date']
    N_JOBS = args['n_jobs']

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    log_filename = os.path.join(LOG_DIR, datetime.today().strftime("%Y-%m-%d.log"))

    logging.basicConfig(level=logging.INFO, filename=log_filename)
    LOGGER = logging.getLogger("Classify_daily")
    LOGGER.info('----' * 20 + "{}".format(datetime.today()))

    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    # have to put cluster, client inside main function
    cluster = LocalCluster(ip="0.0.0.0")
    client = Client(cluster)
    LOGGER.info(client)

    # connect to redis
    redisConn = RedisConnection()
    pool = Pool(N_JOBS)     # set poll to parallel some tasks

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

    # for (filename, class_property) in zip(filename_based_url, class_property_based_url):
    #     process_url_property(DATA_PATH, filename, class_property)
