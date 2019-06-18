#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 15 15:50 2019

@author: phongdk
"""

import redis


class RedisConnection():
    def __init__(self):
        self.host = 'ads-target1v.itim.vn'
        self.password = 'ezzcka2c6s80zwdot6lzu8br63h5beqgbbqwkrmzbgr9k4zftr2xpx5j1v9y10ohy'
        self.port = 6379
        self.connectRedis()

    def connectRedis(self):
        try:
            self.conn = redis.Redis(host=self.host, password=self.password, port=self.port)
        except Exception as err:
            raise err

    def get_browser_id(self, hash_id):
        result = self.conn.hget('uid_mapping', hash_id)
        if result is not None:
            return result.decode("utf-8").split("|")[0]
        return None


if __name__ == '__main__':
    redisConn = RedisConnection()
    print(redisConn.get_browser_id("-6472122476610619770"))
