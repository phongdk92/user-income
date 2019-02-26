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


class AdministrativeArea:
    def __init__(self, path, level=6):
        self.MIN_LAT = 8.33         # Vietnam Boundary
        self.MAX_LAT = 23.400
        self.MIN_LON = 102.074
        self.MAX_LON = 110.001

        self.province_filename = os.path.join(path, 'VNM_adm1.shp')
        self.district_filename = os.path.join(path, 'VNM_adm2.shp')
        self.commune_filename = os.path.join(path, 'VNM_adm3.shp')
        self.provinces = {}
        self.__read_province_shape_file()
        self.__read_district_shape_file()
        self.__read_commune_shape_file()

    def __read_province_shape_file(self):
        sf = shapefile.Reader(self.province_filename)
        for (record, area) in zip(sf.records(), sf.shapes()):
            name = record[4]
            province = {'name': name,
                        'polygon': Polygon(area.points)}
            self.provinces[name] = province

    def __read_district_shape_file(self):
        sf = shapefile.Reader(self.district_filename)
        for (record, area) in zip(sf.records(), sf.shapes()):
            province_name = record[4]
            district_name = record[6]
            district = {'name': district_name,
                        'polygon': Polygon(area.points)}
            self.provinces[province_name][district_name] = district

    def __read_commune_shape_file(self):
        sf = shapefile.Reader(self.commune_filename)
        for (record, area) in zip(sf.records(), sf.shapes()):
            province_name = record[4]
            district_name = record[6]
            commune_name = record[8]
            commune = {'name': commune_name,
                       'polygon': Polygon(area.points)}
            self.provinces[province_name][district_name][commune_name] = commune

    # def check_point_intersect_polygon(self, longitude, latitude):
    #     if latitude > self.MAX_LAT or latitude < self.MIN_LAT or longitude > self.MAX_LON or longitude < self.MIN_LON:
    #         return 'Aboard'
    #
    #     point = Point(longitude, latitude)
    #     for province_name in list(self.provinces.keys()):
    #         if self.provinces[province_name]['polygon'].intersects(point):
    #             province_info = self.provinces[province_name]
    #             for district_name in list(province_info.keys())[2:]:
    #                 if province_info[district_name]['polygon'].intersects(point):
    #                     district_info = province_info[district_name]
    #                     for commune_name in list(district_info.keys())[2:]:
    #                         if district_info[commune_name]['polygon'].intersects(point):
    #                             return district_info[commune_name]['name']

    def find_polygon(self, point, infos, start=0):
        distances = []
        for name in list(infos.keys())[start:]:
            distances.append(point.distance(infos[name]['polygon']))
            if infos[name]['polygon'].intersects(point):
                return name
        min_arg_distance = np.argmin(distances)
        return list(infos.keys())[start + min_arg_distance]

    def find_address(self, latitude, longitude):
        if latitude > self.MAX_LAT or latitude < self.MIN_LAT or longitude > self.MAX_LON or longitude < self.MIN_LON:
            return 'Aboard'

        point = Point(longitude, latitude)
        province_name = self.find_polygon(point, self.provinces, start=0)
        district_name = self.find_polygon(point, self.provinces[province_name], start=2)
        commune_name = self.find_polygon(point, self.provinces[province_name][district_name], start=2)
        return province_name, district_name, commune_name


# class AdministrativeArea2:
#     def __init__(self, path, level=6):
#         self.filename = path
#         self.level = level          # 4: district, 6: district
#         self.polygons = []
#         self.records = []
#         self.__read_shape_file()
#
#     def __read_shape_file(self):
#         sf = shapefile.Reader(self.filename)
#         self.records = sf.records()
#         self.polygons = [Polygon(area.points) for area in sf.shapes()]
#         print(len(self.polygons))
#
#     def check_point_intersect_polygon(self, latitude, longitude):
#         point = Point(longitude, latitude)
#         distances = []
#         for (info, polygon) in zip(self.records, self.polygons):
#             #print(point.distance(polygon))
#             distances.append(point.distance(polygon))
#             if polygon.intersects(point):
#                 return info[self.level]
#         #print(np.min(distances), self.records[np.argmin(distances)])
#         return "This points is outside of Vietnam"


if __name__ == '__main__':
    from_date = '2018-11-14'
    end_date = '2018-11-27'
    PATH = '/home/phongdk/data_user_income_targeting'
    VNM_ADM_PATH = '/home/phongdk/VNM_adm/'
    filename_location = "location_from_{}_to_{}.csv.gz".format(from_date, end_date)

    vn_adm = AdministrativeArea(VNM_ADM_PATH)
    # vn_adm2 = AdministrativeArea2(os.path.join(VNM_ADM_PATH, 'VNM_adm2.shp'))
    # vn_adm3 = AdministrativeArea2(os.path.join(VNM_ADM_PATH, 'VNM_adm3.shp'))

    df = pd.read_csv(os.path.join(PATH, filename_location), nrows=500)
    df.set_index('user_id', inplace=True)

    print(vn_adm.find_address(20.46120, 106.176))
    #df['address'] = df.apply(lambda x: vn_adm.find_address(x['lat'], x['lon']), axis=1)
    #df['address2'] = df.apply(lambda x: vn_adm.check_point_intersect_polygon(x['lat'], x['lon']), axis=1)
    #df['address3'] = df.apply(lambda x: vn_adm3.check_point_intersect_polygon(x['lat'], x['lon']), axis=1)

    #print(df[df['address2'].isnull()])
    #print(df[df['address'] == 'Aboard'])
    # df_outside = df[(df['address'].isnull()) & (df['address3'].isnull())]
    # print(df_outside)
    # df_outside[['lat', 'lon']].to_csv('/home/phongdk/point_outside.csv', index=True)
