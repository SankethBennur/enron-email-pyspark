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
def extract_email_network(rdd_seq):
    
    email_regex =  "[\w!#$%&'+-/=?^_`{|}~]+@([\w]+\.)[\w][a-zA-Z]+[\w]"
    enron_regex = "[@(.)]enron[(.)]"
    space_regex = '(\n\t)'

    snd_rec_vec = lambda x, y, t: [(x, val, t) for val in y]
    
    Q1_rdd_0 = rdd_seq.map(lambda x: Parser().parsestr(x))
    Q1_rdd_1 = Q1_rdd_0.map(lambda x: (x.get('From'), str(x.get('To')).split(', '), date_to_dt(x.get('Date'))))
    Q1_rdd_2 = Q1_rdd_1.flatMap(lambda X: snd_rec_vec(X[0], X[1], X[2]))
    # rdd_2 = rdd_1.flatMap(lambda x: [(x[0], i, x[2]) for i in x[1]])
    
    # To filter Valid and Non-enron mails

    # Valid
    Q1_rdd_3 = Q1_rdd_2.filter(lambda x: (re.search(email_regex, x[0])))
    Q1_rdd_3 = Q1_rdd_3.filter(lambda x: (re.search(email_regex, x[1])))

    # Non-enron mails
    Q1_rdd_3 = Q1_rdd_3.filter(lambda x: re.search(enron_regex, x[0]))
    Q1_rdd_3 = Q1_rdd_3.filter(lambda x: re.search(enron_regex, x[1]))
    
    # Eliminating self-loops
    Q1_rdd_4 = Q1_rdd_3.filter(lambda x: x[0] != x[1])

    # Removing '\n\t' from emails
    Q1_rdd_5 = Q1_rdd_4.map(lambda x: (x[0], x[1].lstrip(space_regex), x[2]))
    
    return Q1_rdd_5

# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    
    def filter_rdd(rdd_1):
        if (drange != None):
            rdd_1 = rdd_1.filter(lambda x: x[2] >= drange[0])
            rdd_1 = rdd_1.filter(lambda x: x[2] <= drange[1])
        
        return rdd_1.collect()
    
    def unique_mails(rdd_mails):
        m = []
        w = []
        weight_mails = []
        
        for pair in rdd_mails:
            if (pair[0], pair[1]) in m:
                w[m.index((pair[0],pair[1]))][2] += 1
            else:
                m.append((pair[0], pair[1]))
                w.append([pair[0], pair[1], 1])

        for i in w: weight_mails.append(tuple(i))

        return weight_mails
    
    return sc.parallelize(unique_mails(filter_rdd(rdd)))


# Q3.1: replace pass with your code
def get_out_degrees(rdd):
    s_mails = []
    g_o_deg = []
    s = []
    
    for mail in rdd.collect():
        if mail[0] in g_o_deg:
            s_mails[g_o_deg.index(mail[0])][0] += 1            
        else:
            g_o_deg.append(mail[0])
            s_mails.append([mail[2], mail[0]])
        
    for i in range(len(s_mails)):
        min_idx = i
        
        for j in range(i+1, len(s_mails)):
            if s_mails[min_idx][0] < s_mails[j][0]:
                min_idx = j
       
        s_mails[i], s_mails[min_idx] = s_mails[min_idx], s_mails[i]
        
    for i in s_mails: s.append(tuple(i))
    
    return sc.parallelize(s)


# Q3.2: replace pass with your code         
def get_in_degrees(rdd):
    s_mails = []
    g_in_deg = []
    s = []
    
    for mail in rdd.collect():
        if mail[1] in g_in_deg:
            s_mails[g_in_deg.index(mail[1])][0] += 1            
        else:
            g_in_deg.append(mail[1])
            s_mails.append([mail[2], mail[1]])
        
    for i in range(len(s_mails)):
        min_idx = i
        
        for j in range(i+1, len(s_mails)):
            if s_mails[min_idx][0] < s_mails[j][0]:
                min_idx = j
       
        s_mails[i], s_mails[min_idx] = s_mails[min_idx], s_mails[i]
        
    for i in s_mails: s.append(tuple(i))
    
    return sc.parallelize(s)

# Q4.1: replace pass with your code            
def get_out_degree_dist(rdd):
    Q4_RDD = get_out_degrees(rdd)
    n = []
    w = []
    
    for weight_mail in Q4_RDD.collect():
        if weight_mail[0] in n:
            w[n.index(weight_mail[0])][1] += 1
        else:
            n.append(weight_mail[0])
            w.append([weight_mail[0], 1])

            
    for i in range(len(w)):
        min_idx = i
        
        for j in range(i+1, len(w)):
            if w[min_idx][0] > w[j][0]:
                min_idx = j
       
        w[i], w[min_idx] = w[min_idx], w[i]
            
    Q4 = []
    for i in w: Q4.append(tuple(i))
        
    return sc.parallelize(Q4)

# Q4.2: replace pass with your code
def get_in_degree_dist(rdd):
    Q4_RDD = get_in_degrees(rdd)
    
    n = []
    w = []
    
    for weight_mail in Q4_RDD.collect():
        if weight_mail[0] in n:
            w[n.index(weight_mail[0])][1] += 1
        else:
            n.append(weight_mail[0])
            w.append([weight_mail[0], 1])

            
    for i in range(len(w)):
        min_idx = i
        
        for j in range(i+1, len(w)):
            if w[min_idx][0] > w[j][0]:
                min_idx = j
       
        w[i], w[min_idx] = w[min_idx], w[i]
            
    Q4 = []
    for i in w: Q4.append(tuple(i))
        
    return sc.parallelize(Q4)
