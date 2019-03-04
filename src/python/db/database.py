#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 06 16:39 2018

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
                        "--password=`cat /home/phongdk/.clickhouse_pw` --host=aggregator3v.dev.itim.vn --query "

CONNECT_TO_BROWSER_STAT = "clickhouse-client --user=default --host=browser-stat1v.dev.itim.vn --query "


def get_data_from_server(connect_to_server, query, external=""):
    command = connect_to_server + "\"{}\" ".format(query) + external

    output = subprocess.check_output(command, shell=True)
    output = output.decode('utf-8', errors='ignore').split('\n')
    output = [x.split('\t') for x in output]
    return output[:-1]


def collect_user_demography_info(filename, from_date, end_date):
    '''
    Only get demography info at the end_date since the demography prediction is accumulated 2 weeks
    :param filename:
    :param from_date:
    :param end_date:
    :return:
    '''

    query = "SELECT " \
            "cityHash64(user_id) as uid, " \
            "gender, " \
            "age " \
            "FROM " \
            "demography.prediction " \
            "WHERE " \
            "event_date = '{}' " \
            "".format(end_date)
    output = get_data_from_server(CONNECT_TO_BROWSER_STAT, query)
    columns = ['user_id', 'gender', 'age']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_url_with_filter_info(filename, from_date, end_date, filter_urls):
    print("PROCESS : {} --- {}".format(filename, filter_urls))
    days_gap = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(from_date, "%Y-%m-%d")).days + 1
    external = "--external --file {} --name='temp_url' --structure='url String'".format(filter_urls)

    output = []
    for day in range(days_gap):
        query_date = datetime.strptime(from_date, "%Y-%m-%d") + timedelta(days=day)
        print('query on date :{}'.format(query_date))
        query = "SELECT " \
                "cityHash64(user_id) as uid, " \
                "event_date, " \
                "count(request) " \
                "FROM " \
                "browser.clickdata " \
                "WHERE " \
                "event_date = '{}' AND " \
                "cutToFirstSignificantSubdomain(request) IN temp_url " \
                "GROUP BY event_date, uid " \
                "ORDER BY event_date, uid " \
                "".format(query_date.strftime("%Y-%m-%d"))
        output.extend(get_data_from_server(CONNECT_TO_AGGREGATOR, query, external))
        print(len(output))
    columns = ['user_id', 'event_date', 'count']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_hardware_info(filename, from_date, end_date):
    print("PROCESS : HARDWARE")
    query = "SELECT " \
            "cityHash64(user_id) as uid, " \
            "os_name, " \
            "sys_ram_mb, " \
            "dictGetString('hw_class', 'name', toUInt64(hw_class)) as hw_class,  "\
            "dictGetString('cpu', 'vendor', toUInt64(cpu)) as cpu, " \
            "dictGetUInt16('screens', 'width', toUInt64(screen)) as screen_width, "\
            "dictGetUInt16('screens', 'height', toUInt64(screen)) as screen_height "\
            "FROM " \
            "browser.metrics " \
            "WHERE " \
            "event_date BETWEEN '{}' AND '{}' " \
            "GROUP BY uid, os_name, sys_ram_mb, hw_class, cpu, screen_width, screen_height " \
            "ORDER BY uid, os_name, sys_ram_mb, hw_class, cpu, screen_width, screen_height " \
            "".format(from_date, end_date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'os_name', 'sys_ram_mb', 'hw_class', 'cpu', 'screen_width', 'screen_height']
    export_to_csv(filename, output, columns)
    del output
    gc.collect()


def collect_user_location(filename, from_date, end_date):
    print("PROCESS : LOCATION")
    query = "SELECT " \
            "cityHash64(user_id) as uid, " \
            "lat, " \
            "lon " \
            "FROM " \
            "browser.metrics " \
            "WHERE " \
            "event_date BETWEEN '{}' AND '{}' " \
            "AND lat > 0 AND lon > 0 " \
            "GROUP BY uid, lat, lon " \
            "ORDER BY uid, lat, lon " \
            "".format(from_date, end_date)
    output = get_data_from_server(CONNECT_TO_AGGREGATOR, query)
    columns = ['user_id', 'lat', 'lon']
    export_to_csv(filename, output, columns)    # the file size is larger than 1 GB --> use DASK to distribute file
    del output
    gc.collect()


def export_to_csv(filename, output, columns):
    print('number of rows : {}'.format(len(output)))
    df = pd.DataFrame.from_records(output)
    df.columns = columns
    # df, _ = reduce_mem_usage(df)
    df.to_csv(os.path.join(PATH, filename), compression='gzip', index=False)


if __name__ == '__main__':
    from_date = '2018-11-14'
    end_date = '2018-11-27'

    PATH = '/home/phongdk/data_user_income_targeting'
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

    """EXTERNAL DATA"""
    EXTERNAL_PATH = os.path.join(os.getcwd(), "external_data", "url_properties")
    FILTER_AIRLINE = os.path.join(EXTERNAL_PATH, "10952_Airplane")
    FILTER_LUXURY = os.path.join(EXTERNAL_PATH, "13993_Luxury")
    FILTER_BOOKING_RESORT = os.path.join(EXTERNAL_PATH, "10960_Resort")
    FILTER_BOOKING_HOTEL = os.path.join(EXTERNAL_PATH, "10954_10959_Hotel")
    FILTER_TOUR = os.path.join(EXTERNAL_PATH, "10957_Tour")
    FILTER_SHOPPING = os.path.join(EXTERNAL_PATH, "shopping")

    collect_user_demography_info(filename_demography, from_date, end_date)
    collect_user_hardware_info(filename_hardware, from_date, end_date)
    collect_user_location(filename_location, from_date, end_date)

    for filename, filter_data in zip([filename_airline, filename_luxury, filename_booking_resort,
                                      filename_booking_hotel, filename_tour, filename_shopping],
                                     [FILTER_AIRLINE, FILTER_LUXURY, FILTER_BOOKING_RESORT,
                                      FILTER_BOOKING_HOTEL, FILTER_TOUR, FILTER_SHOPPING]):
        collect_user_url_with_filter_info(filename, from_date, end_date, filter_data)
