#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Update a redis server cache when an evenement is trigger
# in MySQL replication log
#

from datetime import *
import time
import math
import datetime
import redis
import settings as config
import json
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    WriteRowsEvent,
    UpdateRowsEvent,
)


def main():
    redisConn = redis.Redis(host=config.REDIS_SETTINGS["host"], port=config.REDIS_SETTINGS["port"])

    stream = BinLogStreamReader(
        connection_settings=config.MYSQL_SETTINGS,
        server_id=config.SERVER_ID,
        blocking=True,  # log_file="mysql-bin.000028",  #log_pos=706478611,
        resume_stream=True,
        only_tables=["ax_newdata"],
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

    now = int(math.floor(time.time()))

    for binlogevent in stream:
        prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)
        statPrefix = "%s:%s:" % (binlogevent.schema, "statistics")
        data2redis = {}
        binlogStartTime = datetime.now()
        for row in binlogevent.rows:
            # print str(binlogevent.table)
            if binlogevent.table.find(config.DB_SETTINGS["newdata"]) == -1:
                # print "newdata"
                continue
            elif isinstance(binlogevent, UpdateRowsEvent) and (row["after_values"]["ctime"] < now):
                # if row["after_values"]["symbol"].find("EURGBP") != -1:
                # print "olddata",row["after_values"]["volume"],row["after_values"]["high"],row["after_values"]["symbol"],now,row["after_values"]["ctime"]
                continue  # filter old data
            elif isinstance(binlogevent, UpdateRowsEvent) and (
                        binlogevent.table.find(config.DB_SETTINGS["newdata"]) != -1):
                vals = row["after_values"]
                # print vals
                redisConn.set("lastDbUpdateTime", int(math.floor(time.time())))
                result = {}

                # 如果有 price 字段则使用 price 字段作为当前价格，否则使用买一价格
                result["price"] = vals.get('price')
                if result["price"] is None or result["price"] <= 0:
                    vals["price"] = result["price"] = vals["bid"]

                result["timestamp"] = vals["ctime"]

                for k, v in vals.items():
                    if isinstance(v, basestring):
                        result[k.encode("utf-8")] = v.encode("utf-8")
                    else:
                        result[k.encode("utf-8")] = v

                redisKey = "R_" + vals["symbol"]


                # redisConn.set(redisKey, str(result).replace("'", "\""))
                data2redis[redisKey] = str(result).replace("'", "\"")
                if config.STAT_SETTINGS["enabled"] == "true":
                    vals["date"] = datetime.now()
                    vals["event"] = time.time()
                    redisConn.hmset(prefix + vals["symbol"], vals)
                    redisConn.incr(statPrefix + "ALL")

                    eventTime = int(math.floor(vals["event"]))
                    cTime = vals["ctime"]
                    diffTime = eventTime - cTime
                    # print eventTime,cTime,diffTime
                    if diffTime <= 1:
                        redisConn.incr(statPrefix + "1s")
                    elif diffTime <= 3:
                        redisConn.incr(statPrefix + "3s")
                    elif diffTime <= 5:
                        redisConn.incr(statPrefix + "5s")
                    elif diffTime <= 10:
                        redisConn.incr(statPrefix + "10s")
                    elif diffTime > 10:
                        redisConn.incr(statPrefix + "B10")

                        # print datetime.now(), time.time(), vals["ctime"], vals["symbol"], vals["bid"], vals["price"], vals[
                        #    "ask"], vals[
                        #    "high"], vals["low"]

        redisStartTime = datetime.now()
        data2redisLen = len(data2redis)
        if data2redisLen > 0:
            redisConn.mset(data2redis)
        redisEndTime = datetime.now()
        print 'data to redis length: ' + str(redisStartTime) + 'insert time cost: ' + (
        redisEndTime - redisStartTime).seconds

    stream.close()


if __name__ == "__main__":
    main()
