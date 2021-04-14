#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author: ufac001
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

# Q1: replace pass with your code
def extract_email_network(rdd):
    pass

# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    pass

# Q3.1: replace pass with your code
def get_out_degrees(rdd):
    pass

# Q3.2: replace pass with your code         
def get_in_degrees(rdd):
    pass

# Q4.1: replace pass with your code            
def get_out_degree_dist(rdd):
    pass

# Q4.2: replace pass with your code
def get_in_degree_dist(rdd):
    pass
