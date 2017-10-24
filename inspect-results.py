#!/usr/bin/env python

"""
    inspect-results.py
"""

from __future__ import print_function

import sys
import numpy as np
import ujson as json
from pyspark import SparkContext

import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
plt.rc('axes', axisbelow=True)

# --
# Helpers

def parse_user_location(user_location):
    user_id, n_obs, latitude, longitude, median_abs_deviation = user_location.split('\t')
    return (user_id, {
        "user_id" : user_id,
        "n_obs" : int(n_obs),
        "latitude" : float(latitude),
        "longitude" : float(longitude),
        "median_abs_deviation" : float(median_abs_deviation),
    })

def compute_error(actual, predicted):
    return haversine_distance(
        (actual['latitude'], actual['longitude']),
        (predicted['latitude'], predicted['longitude']),
    )

# --

if __name__ == "__main__":
    
    # --
    # Configuration
    
    config = json.load(open(sys.argv[1]))
    sc = SparkContext(appName=config['app_name'])
    sc.addPyFile('./spatial_helpers.py')
    from spatial_helpers import haversine_distance
    
    # --
    # Load + join actual locations and predicted locations
    
    user_locations = sc.textFile(config['user_locations'])\
        .map(parse_user_location)\
        .filter(lambda x: x[1]['median_abs_deviation'] < config['params']['max_median_abs_deviation'])\
        .filter(lambda x: x[1]['n_obs'] >= config['params']['min_n_messages'])
        
    predictions = sc.textFile(config['predictions']).map(parse_user_location)
    joined = user_locations.join(predictions)
    
    # --
    # Compute + plot errors
    
    joined_local = joined.collect()
    errs = np.array(map(lambda x: compute_error(x[1][0], x[1][1]), joined_local))
    
    cuts = np.linspace(0, 100, 100)
    percentiles = np.percentile(errs, cuts)
    _ = plt.plot(percentiles, cuts)
    _ = plt.xscale('log')
    _ = plt.xlabel('error (km)')
    _ = plt.ylabel('P(error < x)')
    _ = plt.title('Error CDF')
    _ = plt.grid()
    _ = plt.savefig('results.png')
    
    print("median error -> %f km" % np.median(errs))
    print("mean error -> %f km" % np.mean(errs))