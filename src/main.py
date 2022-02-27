#!/usr/bin/env python3
import argparse
import importlib
import os
import sys
import time
import json
from pyspark.sql import SparkSession

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs.zip')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pySpark Job')
    parser.add_argument('--job', type=str, dest='job_name',
                          help="Name of the job module to be launch")  # required=True,
    parser.add_argument('--job-args', nargs='*',
                        help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()
    print('\nCalled with arguments : %s' % args)
    job_args = dict()
    if args.job_args:
        print('\n the list is  '.join(args.job_args))
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print('job_args_tuples: %s' % job_args_tuples)
        job_args = {a[0]: a[1] for a in job_args_tuples}
    #args.job_name = 'dataframe'
    print('\nRunning job %s \n' % (args.job_name))
    spark = SparkSession.builder.master("local[*]").appName(args.job_name).getOrCreate()
    with open('../config/config.json', 'r') as f:
        config_dict = json.load(f)
    job_module = importlib.import_module('jobs.' + args.job_name + '.job')
    start = time.time()
    print('\n Spark session created %s \n' % (spark.sparkContext))
    job_module.main(spark, config_dict)
    end = time.time()
    print("\nExecution of job %s took %s seconds" % (args.job_name, end - start))
