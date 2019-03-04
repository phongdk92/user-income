#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Feb 26 10:42 2019

@author: phongdk
"""

import shapefile  # pip install pyshp
from shapely.geometry import Point  # pip install shapely
from shapely.geometry.polygon import Polygon
import os
import pandas as pd
import numpy as np
import operator


# class AdministrativeArea:
#     def __init__(self, path, level=6):
#         self.MIN_LAT = 8.33         # Vietnam Boundary
#         self.MAX_LAT = 23.400
#         self.MIN_LON = 102.074
#         self.MAX_LON = 110.001
#         self.MAX_DISTANCE = 0.5
#         self.MIN_DISTANCE = 0
#
#         self.province_filename = os.path.join(path, 'VNM_adm1.shp')
#         self.district_filename = os.path.join(path, 'VNM_adm2.shp')
#         self.commune_filename = os.path.join(path, 'VNM_adm3.shp')
#         self.provinces = {}
#         self.__read_province_shape_file()
#         self.__read_district_shape_file()
#         self.__read_commune_shape_file()
#
#     def __read_province_shape_file(self):
#         sf = shapefile.Reader(self.province_filename)
#         for (record, area) in zip(sf.records(), sf.shapes()):
#             name = record[4]
#             province = {'name': name,
#                         'polygon': Polygon(area.points)}
#             self.provinces[name] = province
#
#     def __read_district_shape_file(self):
#         sf = shapefile.Reader(self.district_filename)
#         for (record, area) in zip(sf.records(), sf.shapes()):
#             province_name = record[4]
#             district_name = record[6]
#             district = {'name': district_name,
#                         'polygon': Polygon(area.points)}
#             self.provinces[province_name][district_name] = district
#
#     def __read_commune_shape_file(self):
#         sf = shapefile.Reader(self.commune_filename)
#         for (record, area) in zip(sf.records(), sf.shapes()):
#             province_name = record[4]
#             district_name = record[6]
#             commune_name = record[8]
#             commune = {'name': commune_name,
#                        'polygon': Polygon(area.points)}
#             self.provinces[province_name][district_name][commune_name] = commune
#
#     def find_polygon(self, point, infos, start=0):
#         for name in list(infos.keys())[start:]:
#             if infos[name]['polygon'].intersects(point):
#                 return self.MIN_DISTANCE, name
#
#         # if don't find any polygon intersects with the point, find the nearest polygon by distance
#         distances = []
#         for name in list(infos.keys())[start:]:
#             distances.append(point.distance(infos[name]['polygon']))
#
#         min_arg_distance = np.argmin(distances)
#         #print(distances[min_arg_distance])
#
#         return distances[min_arg_distance], list(infos.keys())[start + min_arg_distance]
#
#     def find_address(self, latitude, longitude):
#         if latitude > self.MAX_LAT or latitude < self.MIN_LAT or longitude > self.MAX_LON or longitude < self.MIN_LON:
#             return 'Aboard'
#
#         point = Point(longitude, latitude)
#         distance, province_name = self.find_polygon(point, self.provinces, start=0)
#         if distance > self.MAX_DISTANCE:
#             return 'Aboad'
#         _, district_name = self.find_polygon(point, self.provinces[province_name], start=2)# keep 'name' and 'polygon'
#         _, commune_name = self.find_polygon(point, self.provinces[province_name][district_name], start=2)
#         return f'{commune_name}, {district_name}, {province_name}'


class AdministrativeArea:
    def __init__(self, path, level=6):
        self.MIN_LAT = 8.33         # Vietnam Boundary
        self.MAX_LAT = 23.400
        self.MIN_LON = 102.074
        self.MAX_LON = 110.001
        self.NUM_SAMPLES_CHECK = 50
        self.MAX_DISTANCE = 0.5
        self.MIN_DISTANCE = 0

        self.province_filename = os.path.join(path, 'VNM_adm1.shp')
        self.district_filename = os.path.join(path, 'VNM_adm2.shp')
        self.commune_filename = os.path.join(path, 'VNM_adm3.shp')
        self.__read_commune_shape_file()

    def __read_commune_shape_file(self):
        sf = shapefile.Reader(self.commune_filename)
        self.communes = []
        for (record, area) in zip(sf.records(), sf.shapes()):
            centroid_poly = Polygon(area.points).centroid.xy
            commune_name = f'{record[8]}, {record[6]}, {record[4]}'
            commune = (float(centroid_poly[0][0]), float(centroid_poly[1][0]), commune_name, Polygon(area.points))
            self.communes.append(commune)
        self.communes_long_lat = sorted(self.communes, key=operator.itemgetter(0, 1))  # sort by longitude, latitude
        self.communes_lat_long = sorted(self.communes, key=operator.itemgetter(1, 0))  # sort by latitude, longitude

    def __find_address_by_coordinate(self, point, coordinate, point_coordinate):
        first_idx, last_idx = 0, len(coordinate) - 1

        while first_idx + 1 < last_idx:
            mid_idx = int((first_idx + last_idx) / 2)
            if point_coordinate < coordinate[mid_idx][0]:
                last_idx = mid_idx
            elif coordinate[mid_idx][0] < point_coordinate:
                first_idx = mid_idx

        index = int((first_idx + last_idx) / 2)
        start_idx = max(0, index - self.NUM_SAMPLES_CHECK)
        end_idx = min(index + self.NUM_SAMPLES_CHECK, len(coordinate)) + 1

        for commune in coordinate[start_idx:end_idx]:       #compute distance to each commune
            if commune[3].intersects(point):
                return self.MIN_DISTANCE, commune[2]    # distance = 0

        distances = []
        for commune in coordinate[start_idx:end_idx]:
            distances.append(point.distance(commune[3]))  # compute distance to polygon
        min_arg_distance = np.argmin(distances)
        return distances[min_arg_distance], coordinate[start_idx + min_arg_distance][2]

    def find_address(self, latitude, longitude):
        if latitude > self.MAX_LAT or latitude < self.MIN_LAT or longitude > self.MAX_LON or longitude < self.MIN_LON:
            return 'Aboard'
        point = Point(longitude, latitude)
        dist_by_long, place_long = self.__find_address_by_coordinate(point, self.communes_long_lat, longitude)
        dist_by_lat, place_lat = self.__find_address_by_coordinate(point, self.communes_lat_long, latitude)

        address = place_lat if dist_by_lat < dist_by_long else place_long
        if min(dist_by_lat, dist_by_long) < self.MAX_DISTANCE:
            #print(min(dist_by_lat, dist_by_long))
            return address
        return 'Aboard'


if __name__ == '__main__':
    from_date = '2018-11-14'
    end_date = '2018-11-27'
    PATH = '/home/phongdk/data_user_income_targeting'
    VNM_ADM_PATH = '/home/phongdk/VNM_adm/'
    filename_location = "location_from_{}_to_{}.csv.gz".format(from_date, end_date)

    vn_adm2 = AdministrativeArea(VNM_ADM_PATH)
    # vn_adm2 = AdministrativeArea2(VNM_ADM_PATH)
    # print(vn_adm2.find_address(20.46120, 106.176))
    print(vn_adm2.find_address(19.3724, 105.9281))
    print(vn_adm2.find_address(17.9618, 102.626))   # Laos
    print(vn_adm2.find_address(10.579, 107.120))  # Ba Ria - Vung Tau
    print(vn_adm2.find_address(10.6521, 107.249))   # Ba Ria - Vung Tau
    exit()
    # vn_adm3 = AdministrativeArea2(os.path.join(VNM_ADM_PATH, 'VNM_adm3.shp'))

    df = pd.read_csv(os.path.join(PATH, filename_location), nrows=5000)
    df.set_index('user_id', inplace=True)
    print('--------Finding address----------------')
    df['address'] = df.apply(lambda x: vn_adm2.find_address(x['lat'], x['lon']), axis=1)
    #print(df.head(15))
    print(df.tail(15))
    df.to_csv(os.path.join(PATH, 'address_fast.csv.gz'), compression='gzip', index=True)
    #print(vn_adm.find_address(20.46120, 106.176))
