#!/usr/bin/env python3

import time
import random

import __main__ as main

from datetime import datetime, timedelta, MINYEAR
import pymongo
from pymongo import uri_parser, MongoClient, ASCENDING, DESCENDING, ReturnDocument, CursorType
from pytz import utc


def setup(db):
    data = []
    for _ in range(100):
        data.append(random.random())

    extra = []
    for _ in range(1000):
        extra.append(random.random())

    for i in range(200000):
        if i % 1000 == 0:
            print(i)
        db.pybench.test.update(
            {"_id": i},
            {
                "$set": {
                    "initial": random.random(), "data": data, "extra": extra
                },
                "$inc": {
                    "count": 1
                }
            },
            upsert=True
        )

def thrash_index(db):
    for i in range(1000):
        print(i)
        db.pybench.test.create_index(
            [("data", ASCENDING)],
            background=False)
        db.pybench.test.drop_index(
            [("data", ASCENDING)])

def work():
    db = MongoClient("mongodb://localhost:40000", tz_aware=True)

    setup(db)
    thrash_index(db)

def main():
    work()

if __name__ == "__main__":
    main()



