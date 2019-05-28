#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 24 15:37 2019

@author: phongdk
"""

import subprocess
import pandas as pd
import numpy as np
import os
import gc
from datetime import datetime, timedelta
from memory_saving import reduce_mem_usage

CONNECT_TO_AGGREGATOR = "clickhouse-client --progress --user=stats_webui " \
                        "--password=`cat /home/phongdk/.clickhouse_pw` --host=st-ch.itim.vn --query "


def get_data_from_server(connect_to_server, query, external=""):
    command = connect_to_server + "\"{}\" ".format(query) + external
    output = subprocess.check_output(command, shell=True)
    output = output.decode('utf-8', errors='ignore').split('\n')
    output = [x.split('\t') for x in output]
    return output[:-1]


def collect_user_demography_info(filename, date):
    '''
    Only get demography info at the date since the demography prediction is accumulated 2 weeks
    '''
    print("PROCESS : DEMOGRAPHY --- ")
    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "gender, " \
            "age " \
            "FROM " \
            "demography.prediction " \
            "WHERE " \
            "event_date = '{}' " \
            "".format(date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'gender', 'age']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_url_with_filter_info(filename, date, filter_urls):
    print("PROCESS : {} --- {}".format(filename, filter_urls))
    external = "--external --file {} --name='temp_url' --structure='url String'".format(filter_urls)
    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "event_date, " \
            "count(request) " \
            "FROM " \
            "browser.clickdata " \
            "WHERE " \
            "event_date = '{}' AND " \
            "cutToFirstSignificantSubdomain(request) IN temp_url " \
            "GROUP BY event_date, uid " \
            "ORDER BY event_date, uid " \
            "".format(date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query, external)
    columns = ['user_id', 'event_date', 'count']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_hardware_info(filename, date):
    print("PROCESS : HARDWARE")
    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "os_name, " \
            "sys_ram_mb, " \
            "dictGetString('hw_class', 'name', toUInt64(hw_class)) as hw_class,  "\
            "dictGetString('cpu', 'vendor', toUInt64(cpu)) as cpu, " \
            "dictGetUInt16('screens', 'width', toUInt64(screen)) as screen_width, "\
            "dictGetUInt16('screens', 'height', toUInt64(screen)) as screen_height "\
            "FROM " \
            "browser.metrics " \
            "WHERE " \
            "event_date = '{}' " \
            "GROUP BY uid, os_name, sys_ram_mb, hw_class, cpu, screen_width, screen_height " \
            "ORDER BY uid, os_name, sys_ram_mb, hw_class, cpu, screen_width, screen_height " \
            "".format(date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'os_name', 'sys_ram_mb', 'hw_class', 'cpu', 'screen_width', 'screen_height']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_location(filename, date):
    print("PROCESS : LOCATION")
    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "regions[-2] " \
            "FROM browser_clicks.data " \
            "WHERE " \
            "event_date = '{}' " \
            "AND length(regions) >= 2 " \
            "GROUP BY uid, regions " \
            "ORDER BY uid, regions " \
            "".format(date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'location']
    export_to_csv(filename, output, columns)    # the file size is larger than 1 GB --> use DASK to distribute file
    del output
    gc.collect()


def collect_user_payment_success(filename, date, filter_urls):
    print("PROCESS : --------------- PAYMENT_SUCCESS -------------------")
    df = pd.read_csv(filter_urls, header=None, names=['website'])
    websites = " OR ".join(["redirect like '{}' OR request like '{}'".format(x, x) for x in df["website"].values])
    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "count(uid) " \
            "FROM browser.clickdata " \
            "WHERE event_date = '{}' " \
            "AND ({}) " \
            "GROUP BY uid " \
            "".format(date, websites)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'count']
    export_to_csv(filename, output, columns)  # the file size is larger than 1 GB --> use DASK to distribute file


def collect_user_daily_histogram(filename, date, filter_urls):
    print("PROCESS : --------------- DAILY_HISTOGRAM -------------------")
    external = "--external --file {} --name='temp_url' --structure='url String'".format(filter_urls)

    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "toHour(event_time) as hour, " \
            "count(request) " \
            "FROM browser.clickdata " \
            "WHERE event_date='{}' " \
            "AND transition != 255 " \
            "AND domainWithoutWWW(request) NOT IN temp_url " \
            "GROUP BY uid, hour " \
            "ORDER BY uid, hour ".format(date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query, external)
    columns = ['user_id', 'hour', 'count']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def export_to_csv(filename, output, columns):
    print('number of rows : {}'.format(len(output)))
    df = pd.DataFrame.from_records(output)
    print(df.head())
    df.columns = columns
    # df, _ = reduce_mem_usage(df)
    df.to_csv(os.path.join(PATH, filename), compression='gzip', index=False)


if __name__ == '__main__':
    #  '2019-04-30'
    start_date = '2019-05-21'
    for day in range(1):
        date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=day)).strftime("%Y-%m-%d")
        print(date)
        PATH = '/home/phongdk/data_user_income_targeting/data/{}'.format(date)
        if not os.path.exists(PATH):
            os.makedirs(PATH)

        filename_demography = "demography_{}.gz".format(date)
        filename_hardware = "hardware_{}.gz".format(date)
        filename_location = "location_{}.gz".format(date)

        filename_airline = "airline_{}.gz".format(date)
        filename_luxury = "luxury_{}.gz".format(date)
        filename_booking_resort = "booking_resort_{}.gz".format(date)
        filename_booking_hotel = "booking_hotel_{}.gz".format(date)
        filename_tour = "tour_{}.gz".format(date)
        filename_shopping = "shopping_{}.gz".format(date)

        filename_payment = "payment_{}.gz".format(date)
        filename_daily_historgram = "daily_histogram_{}.gz".format(date)

        """EXTERNAL DATA"""
        EXTERNAL_PATH = os.path.join(os.getcwd(), "external_data", "url_properties")
        FILTER_AIRLINE = os.path.join(EXTERNAL_PATH, "10952_Airplane")
        FILTER_LUXURY = os.path.join(EXTERNAL_PATH, "13993_Luxury")
        FILTER_BOOKING_RESORT = os.path.join(EXTERNAL_PATH, "10960_Resort")
        FILTER_BOOKING_HOTEL = os.path.join(EXTERNAL_PATH, "10954_10959_Hotel")
        FILTER_TOUR = os.path.join(EXTERNAL_PATH, "10957_Tour")
        FILTER_SHOPPING = os.path.join(EXTERNAL_PATH, "shopping")
        FILTER_PAYMENT = os.path.join(EXTERNAL_PATH, "shopping_payment_success")
        FILTER_DAILY_HISTORAM = os.path.join(EXTERNAL_PATH, "bad_domains_for_user_log")

        # collect_user_demography_info(filename_demography, date)
        collect_user_hardware_info(filename_hardware, date)
        collect_user_location(filename_location, date)

        for filename, filter_data in zip([filename_airline, filename_luxury, filename_booking_resort,
                                          filename_booking_hotel, filename_tour, filename_shopping],
                                         [FILTER_AIRLINE, FILTER_LUXURY, FILTER_BOOKING_RESORT,
                                          FILTER_BOOKING_HOTEL, FILTER_TOUR, FILTER_SHOPPING]):
            collect_user_url_with_filter_info(filename, date, filter_data)

        collect_user_payment_success(filename_payment, date, FILTER_PAYMENT)
        collect_user_daily_histogram(filename_daily_historgram, date, FILTER_DAILY_HISTORAM)