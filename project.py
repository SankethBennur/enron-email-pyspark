from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta
import operator

# import pyspark
# sc = pyspark.SparkContext()

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))



def extract_email_network(rdd):
    
    email_regex =  "[\w!#$%&'+-/=?^_`{|}~]+@([\w]+\.)[\w][a-zA-Z]+[\w]"
    enron_regex = "[@(.)]enron[(.)]"
    space_regex = '(\n\t)'

    snd_rec_vec = lambda x, y, t: [(x, val, t) for val in y]
    
    Q1_RDD = rdd.map(lambda x: Parser().parsestr(x)) \
                .map(lambda x: (x.get('From'), str(x.get('To')).split(', '), date_to_dt(x.get('Date')))) \
                .flatMap(lambda X: snd_rec_vec(X[0], X[1], X[2]))
    
    # To filter Valid and Non-enron mails

    # Valid
    Q1_RDD = Q1_RDD.filter(lambda x: (re.search(email_regex, x[0]))) \
                    .filter(lambda x: (re.search(email_regex, x[1])))

    # Non-enron mails
    Q1_RDD = Q1_RDD.filter(lambda x: re.search(enron_regex, x[0])) \
                    .filter(lambda x: re.search(enron_regex, x[1]))
    
    # Eliminating self-loops
    Q1_RDD = Q1_RDD.filter(lambda x: x[0] != x[1])

    # Removing '\n\t' from emails
    Q1_RDD = Q1_RDD.map(lambda x: (x[0], x[1].lstrip(space_regex), x[2]))
    
    return Q1_RDD


def convert_to_weighted_network(rdd, drange=None):

    if (drange != None):
        rdd = rdd.filter(lambda x: x[2] >= drange[0])
        rdd = rdd.filter(lambda x: x[2] <= drange[1])

    Q2_RDD = rdd.map(lambda x: ((x[0], x[1]), 1)) \
                .reduceByKey(operator.add) \
                .map(lambda x: (x[0][0], x[0][1], x[1]))
    
    return Q2_RDD


def get_out_degrees(rdd):
    
    Q3i_RDD = rdd.map(lambda x: (x[0], x[2])) \
                    .reduceByKey(operator.add) \
                    .map(lambda x: (x[1], x[0])) \
                    .sortBy(lambda x: x[0], ascending = False)

    return Q3i_RDD



def get_in_degrees(rdd):
    
    Q3ii_RDD = rdd.map(lambda x: (x[1], x[2])) \
                    .reduceByKey(operator.add) \
                    .map(lambda x: (x[1], x[0])) \
                    .sortBy(lambda x: x[0], ascending = False)

    return Q3ii_RDD



def get_out_degree_dist(rdd):
    Q4i_RDD = get_out_degrees(rdd)
    Q4i_RDD = Q4i_RDD.map(lambda x: (x[0], 1)) \
                        .reduceByKey(operator.add) \
                        .sortBy(lambda x: x[0])

    return Q4i_RDD


def get_in_degree_dist(rdd):
    Q4ii_RDD = get_in_degrees(rdd)
    Q4ii_RDD = Q4ii_RDD.map(lambda x: (x[0], 1)) \
                        .reduceByKey(operator.add) \
                        .sortBy(lambda x: x[0])

    return Q4ii_RDD

