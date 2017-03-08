#!/usr/bin/env python3

import logging
import argparse
import hjson
import appdirs
import time
import threading
import pymongo
import bz2
import json
import bson
import collections
import random
import hjson
import ijson
from haversine import haversine
import argparse
import sys
import os
import warnings

import __main__ as main

from datetime import datetime, timedelta, MINYEAR
import pymongo
from pymongo import uri_parser, MongoClient, ASCENDING, DESCENDING, ReturnDocument, CursorType
from pytz import utc

from queue import Queue

VERSION = "staging"

logger = logging.getLogger(__name__)

_externals = {
}

def ts2date(timestamp):
    return utc.localize(datetime.utcfromtimestamp(timestamp / 1000))

class Interval(object):
    """Return true once every N seconds, regardless of how many times we have been checked."""
    def __init__(self, seconds):
        self.interval = seconds
        self.last_checked = time.time()
        self.creation_time = self.last_checked

    def expired(self):
        now = time.time()
        if now - self.last_checked > self.interval:
            self.last_checked = now
            return True
        else:
            return False

    def wait(self):
        now = time.time()
        if now - self.last_checked < self.interval:
            time.sleep(now - self.last_checked)

    def getTotalElapsedTime(self):
        return time.time() - self.creation_time


def defaultBulkErrorHandler(error):
    logger.error("Bulk write error: %s", error.details)

class QueuedExecution(object):
    """Create a thread pool and allow jobs to be submitted to that pool for execution."""
    # pylint: disable=too-many-instance-attributes
    class Exit(object):                 # pylint: disable=locally-disabled,too-few-public-methods
        pass

    def __init__(
            self,
            target,
            thread_count=10,
            bulk_collection=None,
            expected=0,
            bulk_error_handler=defaultBulkErrorHandler):
        self.thread_count = thread_count
        self.target = target
        self.queue = Queue(maxsize=max(100, thread_count * 4))
        self.threads = []
        self.interval = None
        self.interval_lock = threading.Lock()
        self.name = None
        self.bulk_collection = bulk_collection
        self.bulk_error_handler = bulk_error_handler
        self.queue_stats = {
            "expected": expected,
            "processed":  0
        }


        for _ in range(0, thread_count):
            t = threading.Thread(target=self.workerFunction)
            t.start()
            self.threads.append(t)


    def flushBulk(self, bulk):
        try:
            bulk.execute()
        except pymongo.errors.InvalidOperation:
            pass # can occur if there is nothing to do.
        except pymongo.errors.BulkWriteError as bwe:
            self.bulk_error_handler(bwe)
    def workerFunction(self):
        if self.bulk_collection:
            bulk = mydb[self.bulk_collection].initialize_unordered_bulk_op()
            bulk_count = 0
        else:
            bulk = None
        while True:
            args, kwargs = self.queue.get()
            self.checkInterval(processed=1)
            if isinstance(args[0], QueuedExecution.Exit):     # We've been asked to exit
                if bulk:
                    self.flushBulk(bulk)
                break
            if bulk:
                self.target(bulk=bulk, *args, **kwargs)
                bulk_count += 1
                if bulk_count % 1000 is 0:
                    self.flushBulk(bulk)
                    bulk = mydb[self.bulk_collection].initialize_unordered_bulk_op()
                    bulk_count = 0
            else:
                self.target(*args, **kwargs)


    def submit(self, *args, **kwargs):
        self.checkInterval()
        self.queue.put((args, kwargs))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # let each thread know there is no more work
        for _ in range(0, self.thread_count):
            self.submit(QueuedExecution.Exit())

        # Wait for all threads to end
        for t in self.threads:
            t.join()

    def setInterval(self, name, seconds=5):
        if self.interval:
            del self.interval
        self.interval = Interval(seconds)
        self.name = name

    def checkInterval(self, processed=0):
        if self.interval:
            self.interval_lock.acquire()
            self.queue_stats["processed"] += processed
            if self.interval.expired():
                logger.debug("%s, %d elapsed [rate: %d/s, processed: %d, duplicates: %d].",
                             self.name,
                             self.interval.getTotalElapsedTime(),
                             int(self.queue_stats["processed"] / self.interval.getTotalElapsedTime()),
                             self.queue_stats["processed"],
                             duplicates)
                """
                logger.debug("%s in progress, %d seconds elapsed so far.  [expected: %d, processed: %d, duplicates: %d].",
                             self.name,
                             self.interval.getTotalElapsedTime(),
                             self.queue_stats["expected"],
                             self.queue_stats["processed"],
                             duplicates)
                """
            self.interval_lock.release()


def logging_numeric_level(level):
    """Return the numeric log level given a standard keyword"""
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise SystemExit("invalid log level: {}".format(level))
    return numeric_level


def setup_logging(root, log_level, log_times):
    logging.Formatter.converter = time.gmtime
    logformat = "%(levelname)s: [%(funcName)s] %(message)s"
    datelogformat = "%(asctime)s.%(msecs)03dZ - " + logformat
    datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(
        filename=root + ".log",
        format=datelogformat,
        level=logging_numeric_level(log_level),
        datefmt=datefmt)

    # set logging to occur to stdout as well as to the file
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(
        datelogformat if log_times else logformat,
        datefmt=datefmt))

    root = logging.getLogger()
    root.setLevel(logging_numeric_level(log_level))
    root.addHandler(ch)

    # quiet down logging for requests and urllib3 and ignore verify=False warning
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    #requests.packages.urllib3.disable_warnings()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {}".format(VERSION))
    parser.add_argument(
        "--config-path",
        default=default_config_path,
        help="configuration info")
    parser.add_argument(
        "--log-level",
        help="specify logging level")
    parser.add_argument(
        "--log-times",
        action="store_true",
        help="timestamp console logs")
    return parser.parse_args()

def checkStatus(element, now, document):
    age = (now - document["recorded"]).total_seconds()
    if age > _externals["thresholds"][element]:
        text = "{} scanning has exceeded threshold ({:0.1f}s)".format(element, age)
        logging.info(text)
        sendToHangouts(text, _externals)
        sendToSlack(text, _externals)
        sendToTelegram(text, _externals)
        sendToPushover(text, _externals)

def XXXtemplate():
    db = MongoClient(
            _externals["mongo"]["uri"], tz_aware=True)[uri_parser.parse_uri(
                 _externals["mongo"]["uri"])["database"]]

    now = datetime.now(tz=utc)
    last_week = datetime.now(tz=utc) - timedelta(days=7)

    last_action = db.Chat.Public.find_one({"recorded": {"$gt": last_week}}, sort=[("recorded", DESCENDING)])
    checkStatus("actions", now, last_action)

    last_event = db.Norad.Events.find_one({}, sort=[("recorded", DESCENDING)])
    checkStatus("events", now, last_event)

    last_portal = db.Portals.find_one({}, sort=[("recorded", DESCENDING)])
    checkStatus("portals", now, last_portal)

def setup(db):

    data = []
    for _ in range(1000):
        data.append(random.random())

    for i in range(200000):
        if i % 1000 == 0:
            print(i)
        db.pybench.test.update(
            {"_id": i},
            {
                "$set": {
                    "initial": random.random(), "data": data
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
    (root, ext) = os.path.splitext(os.path.basename(__file__))
    dirs = appdirs.AppDirs(root)
    global default_config_path         # pylint: disable=global-statement
    default_config_path = os.path.join(dirs.user_config_dir, "config.json")

    args = parse_args()

    if args.config_path:
        _externals["config_path"] = args.config_path
    _externals["config_path"] = os.path.abspath(os.path.expanduser(_externals["config_path"]))

    try:
        with open(_externals["config_path"], "r") as cfg:
            _externals.update(hjson.load(cfg))
    except IOError:
        pass

    if args.log_level:
        setup_logging(root, args.log_level, args.log_times)
    elif "log_level" in _externals:
        setup_logging(root, _externals["log_level"], args.log_times)
    else:
        setup_logging(root, "INFO", args.log_times)

    work()



if __name__ == "__main__":
    main()



