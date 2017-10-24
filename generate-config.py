#!/usr/bin/env python

"""
    generate-config.py
"""

import os
import json
import argparse
from collections import OrderedDict

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--app-name', type=str, default='wai:%s' % os.environ['HOME'].split('/')[-1])
    
    parser.add_argument('--inpath', type=str, required=True)
    parser.add_argument('--outpath', type=str, required=True)
    
    parser.add_argument('--max-median-abs-deviation', type=float, default=30)
    parser.add_argument('--non-reciprocal-edges', action="store_true")
    parser.add_argument('--min-n-messages', type=int, default=3)
    parser.add_argument('--num-iter', type=int, default=1)
    parser.add_argument('--coalesce', type=int, default=64)
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    config = OrderedDict([
        ("app_name",       args.app_name),
        
        ("inpath",         args.inpath),
        
        ("graph",             os.path.join(args.outpath, "graph")),
        ("message_locations", os.path.join(args.outpath, "message_locations")),
        ("user_locations",    os.path.join(args.outpath, "user_locations")),
        ("predictions",       os.path.join(args.outpath, "predictions")),
        
        ("params", OrderedDict([
            ("reciprocal_edges_only", False if args.non_reciprocal_edges else True),
            ("max_median_abs_deviation", args.max_median_abs_deviation),
            ("min_n_messages", args.min_n_messages),
            ("num_iter", args.num_iter),
            ("coalesce", 64)
        ]))
    ])
    
    print json.dumps(config, indent=2)