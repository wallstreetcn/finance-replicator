#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Update a redis server cache when an evenement is trigger
# in MySQL replication log
#

from datetime import *
import time
import math
import string
import redis
import config

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    WriteRowsEvent,
    UpdateRowsEvent,
)


def main():
    r = redis.Redis(host = config.REDIS_SETTINGS["host"],port = config.REDIS_SETTINGS["port"])

    stream = BinLogStreamReader(
        connection_settings=config.MYSQL_SETTINGS,
        server_id=4,
        blocking=True,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
   
    now = int(math.floor(time.time()))

    for binlogevent in stream:
        prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)
        statPrefix = "%s:%s:" % (binlogevent.schema,"statistics")

        for row in binlogevent.rows:
	    if isinstance(binlogevent, UpdateRowsEvent) and (row["after_values"]["ctime"] < now):
			continue#filter old data
        elif isinstance(binlogevent, UpdateRowsEvent) and (binlogevent.table.find(config.DB_SETTINGS["newdata"]) != -1):
            vals = row["after_values"]	
			
			r.set("lastDbUpdateTime",int(math.floor(time.time())))
			result = {}
			for k,v in vals.items():
				if isinstance(v,basestring):
					result[k.encode("utf-8")] = v.encode("utf-8")
				else:
					result[k.encode("utf-8")] = v 
			r.set("R_" + vals["symbol"],result)
					
			if config.STAT_SETTINGS["enabled"] == "true":	
				vals["date"] = datetime.now()
				vals["event"] = time.time()
						r.hmset(prefix + vals["symbol"], vals)
				r.incr(statPrefix + "ALL")		

				eventTime = int(math.floor(vals["event"]))
				cTime = vals["ctime"]
				diffTime = eventTime - cTime
				#print eventTime,cTime,diffTime
				if diffTime <= 1:
					r.incr(statPrefix + "1s")
				elif diffTime <= 3:
					r.incr(statPrefix + "3s")
				elif diffTime <= 5:
					r.incr(statPrefix + "5s")
				elif diffTime <= 10:
					r.incr(statPrefix + "10s")
				elif diffTime > 10:
					r.incr(statPrefix + "B10")

		
			print datetime.now(),time.time(),vals["ctime"],vals["symbol"],vals["bid"],vals["ask"],vals["high"],vals["low"]
            '''elif isinstance(binlogevent, UpdateRowsEvent) and (binlogevent.table.find(config.DB_SETTINGS["finance"]) != -1):
                vals = row["after_values"]
                r.hmset(prefix + str(vals["id"]), vals)
                #print datetime.now(),time.time(),vals["symbol"],vals["open"],vals["prevClose"]
                print datetime.now(),time.time(),vals

	'''	
    stream.close()


if __name__ == "__main__":
    main()
