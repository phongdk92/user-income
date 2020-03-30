#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 24 15:37 2019

@author: phongdk
"""

import subprocess
import pandas as pd
import os
import gc
import argparse
import logging
from datetime import datetime, timedelta
# from memory_saving import reduce_mem_usage

# CONNECT_TO_AGGREGATOR = "clickhouse-client --progress --user=stats_ads_targeting " \
#                        "--password=`cat /home/phongdk/.clickhouse_stats_pw` --host=st-ch.itim.vn --query "


CONNECT_TO_AGGREGATOR = "clickhouse-cli --stacktrace --host=https://ch.itim.vn --user=phongdk-stats " \
			"--arg-password=`head -n 1 /home/phongdk/.clickhouse_pw` --port 443 --query "
LOG_DIR = "logs"


def get_data_from_server(connect_to_server, query, external=""):
    command = connect_to_server + "\"{}\" ".format(query) + external
    print(command)
    output = subprocess.check_output(command, shell=True)
    output = output.decode('utf-8', errors='ignore').split('\n')
    output = [x.split('\t') for x in output]
    return output[:-1]


def collect_user_demography_info(filename, date):
    '''
    Only get demography info at the date since the demography prediction is accumulated 2 weeks
    '''
    LOGGER.info("PROCESS : DEMOGRAPHY --- ")
    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "gender, " \
            "age " \
            "FROM " \
            "demography.prediction " \
            "WHERE " \
            "event_date = '{}' " \
            "GROUP BY uid, gender, age ".format(date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'gender', 'age']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_url_with_filter_info(filename, date, filter_urls):
    LOGGER.info("PROCESS : {} --- {}".format(filename, filter_urls))
    with open(filter_urls, 'r') as f:
        urls = tuple([line.replace("\n", "") for line in f])

    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "event_date, " \
            "count(request) " \
            "FROM " \
            "browser.clickdata " \
            "WHERE " \
            "event_date = '{}' AND " \
            "cutToFirstSignificantSubdomain(request) IN {} " \
            "GROUP BY event_date, uid " \
            "ORDER BY event_date, uid " \
            "".format(date, urls)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'event_date', 'count']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_hardware_info(filename, date):
    LOGGER.info("PROCESS : HARDWARE")
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
    LOGGER.info("PROCESS : LOCATION")
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
    LOGGER.info("PROCESS : --------------- PAYMENT_SUCCESS -------------------")
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
    LOGGER.info("PROCESS : --------------- DAILY_HISTOGRAM -------------------")
    with open(filter_urls, 'r') as f:
        urls = tuple([line.replace("\n", "") for line in f])

    query = "SELECT " \
            "toInt64(cityHash64(user_id)) as uid, " \
            "toHour(event_time) as hour, " \
            "count(request) " \
            "FROM browser.clickdata " \
            "WHERE event_date='{}' " \
            "AND transition != 255 " \
            "AND domainWithoutWWW(request) NOT IN {} " \
            "GROUP BY uid, hour " \
            "ORDER BY uid, hour ".format(date, urls)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'hour', 'count']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def export_to_csv(filename, output, columns):
    LOGGER.info('number of rows : {}'.format(len(output)))
    df = pd.DataFrame.from_records(output)
    LOGGER.info(df.head())
    df.columns = columns
    # df, _ = reduce_mem_usage(df)
    df.to_csv(os.path.join(DATA_PATH, filename), compression='gzip', index=False)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--date", required=True, help="date to run")
    ap.add_argument("-p", "--data_path", required=True, help="path to input folder")

    args = vars(ap.parse_args())

    date = args['date']
    DATA_PATH = args['data_path']
    DATA_PATH = os.path.join(DATA_PATH, date)

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    log_filename = os.path.join(LOG_DIR, datetime.today().strftime("%Y-%m-%d.log"))

    logging.basicConfig(level=logging.INFO, filename=log_filename)
    LOGGER = logging.getLogger("Collect data daily")
    LOGGER.info('----' * 20 + "{}".format(datetime.today()))

    LOGGER.info(date)
    LOGGER.info(DATA_PATH)
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

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

    try:
        collect_user_demography_info(filename_demography, date)
    except:
        print("------------- Cannot collect data for DEMOGRAPHY -----------------")
        LOGGER.info("------------- Cannot collect data for DEMOGRAPHY -----------------")

    try:
        collect_user_hardware_info(filename_hardware, date)
    except:
        LOGGER.info("------------- Cannot collect data for HARDWARE -----------------")

    try:
        collect_user_location(filename_location, date)
    except:
        LOGGER.info("-------------- Cannot collect data for LOCATION ----------------")

    try:
        collect_user_payment_success(filename_payment, date, FILTER_PAYMENT)
    except:
        LOGGER.info("-------------- Cannot collect data for PAYMENT ----------------")

    try:
        collect_user_daily_histogram(filename_daily_historgram, date, FILTER_DAILY_HISTORAM)
    except:
        LOGGER.info("-------------- Cannot collect data for HISTOGRAM ----------------")

    for filename, filter_data in zip([filename_airline, filename_luxury, filename_booking_resort,
                                      filename_booking_hotel, filename_tour, filename_shopping],
                                     [FILTER_AIRLINE, FILTER_LUXURY, FILTER_BOOKING_RESORT,
                                      FILTER_BOOKING_HOTEL, FILTER_TOUR, FILTER_SHOPPING]):
        try:
            collect_user_url_with_filter_info(filename, date, filter_data)
        except:
            LOGGER.info(f"-------------- Cannot collect data for {filename} ----------------")
