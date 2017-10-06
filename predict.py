#!/usr/bin/env python

'''
    predict.py
'''

import sys
import ujson as json
from pyspark import SparkContext

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

def parse_wedge(wedge, drop_weight=True):
    poster, tagged_user, weight = wedge.split('\t')
    
    if drop_weight:
        return (poster, tagged_user)
    else:
        return (poster, tagged_user, int(weight))

def format_user_location(obj):
    user_id, user_location = obj
    return '\t'.join(map(str, (
        user_id,
        user_location['n_obs'],
        user_location['latitude'],
        user_location['longitude'],
        user_location['median_abs_deviation'],
    )))

def spatial_label_propagation(edges, user_locations):
    """
        For each user, compute the spatial median of all of their neighbors
    """
    user_locations = user_locations.mapValues(lambda x: (x['latitude'], x['longitude']))
    neighbor_locations = edges.join(user_locations).values().groupByKey()
    return neighbor_locations.mapValues(spatial_center)


if __name__ == "__main__":
    
    # --
    # Configuration
    
    config = json.load(open(sys.argv[1]))
    sc = SparkContext(appName=config['app_name'])
    sc.addPyFile('./spatial_helpers.py')
    from spatial_helpers import spatial_center, haversine_distance
    
    
    # --
    # Load user locations
    
    user_locations = sc.textFile(config['user_locations']).map(parse_user_location)
    
    # Filter users w/ median_abs_deviation >= some threshold
    user_locations = user_locations.filter(lambda x: x[1]['median_abs_deviation'] < config['params']['max_median_abs_deviation'])
    
    # Filter users w/ n_messages < some threshold
    user_locations = user_locations.filter(lambda x: x[1]['n_obs'] >= config['params']['min_n_messages'])
    
    # Cache for future user
    user_locations.cache()
    
    
    # --
    # Load graph
    
    edges = sc.textFile(config['graph']).map(parse_wedge)
    
    # Subset to bidirectional edges
    if config['params']['reciprocal_edges_only']:
        edges.cache()
        edges = edges.intersection(edges.map(lambda x: (x[1], x[0])))
    
    
    # --
    # Predict
    
    # Iterate
    user_predictions = user_locations
    for _ in range(config['params']['num_iter']):
        user_predictions = spatial_label_propagation(edges, user_predictions)
    
    # Save to disk
    user_predictions.map(format_user_location).saveAsTextFile(config['predictions'])

