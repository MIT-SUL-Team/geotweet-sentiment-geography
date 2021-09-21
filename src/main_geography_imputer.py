# usage: python src/main_geography_imputer.py --data_path /n/holyscratch01/cga/nicogj/geo_input/

import argparse
import pandas as pd
import pymapd
import glob
import os
import numpy as np
from tqdm.auto import tqdm

from utils.data_read_in import read_in

def impute_geography(file, args):

    # Read in Twitter data
    df = read_in(file, args.data_path, cols=['message_id', 'latitude', 'longitude'])
    args.c.execute(
        '''
        DROP TABLE IF EXISTS geotweets;
        '''
    )
    args.con.load_table("geotweets", df, create='infer', method='arrow')

    # Save with geography
    args.c.execute(
        '''
        COPY (
            SELECT
                a.message_id,
                b.OBJECTID,
                b.ID_0,
                b.NAME_0,
                b.ISO,
                b.ID_1,
                b.NAME_1,
                b.ID_2,
                b.NAME_2
            FROM geotweets AS a,
            adm2 AS b
            WHERE ST_Intersects(
                b.omnisci_geo, ST_SetSRID(ST_Point(a.longitude, a.latitude), 4326)
            )
        )
        TO '{}' WITH (delimiter = '\t', quoted = 'true', header='true');
        '''.format(os.path.join(args.output_path, "geography_{}".format(file.replace('.gz', '')))).replace('\n', ' ')
    )

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', default='', help='What filename do you want to impute sentiment for? If none, then all in data path.')
    parser.add_argument('--data_path', default="/n/holyscratch01/cga/nicogj/geo_input/", type=str, help='path to data')
    parser.add_argument('--output_path', default="/n/holyscratch01/cga/nicogj/geo_output/", type=str, help='path to save data')
    parser.add_argument('--platform', default='', help='Which social media data are we using (twitter, weibo)?')
    parser.add_argument('--host_name', default="holygpu7c26204.rc.fas.harvard.edu", type=str, help='OmniSci host name')
    parser.add_argument('--port_number', default=7083, type=int, help='OmniSci port number')
    args = parser.parse_args()

    print("Connecting to Omnisci")
    args.con = pymapd.connect(
        user="admin", password="HyperInteractive", host=args.host_name, port=args.port_number, dbname="omnisci"
    )
    args.c = args.con.cursor()
    print("Connected", args.con)

    if args.filename == '':
        args.files = sorted([os.path.basename(elem) for elem in glob.glob(os.path.join(args.data_path, "*"))])
    else:
        args.files = [args.filename]

    for file in tqdm(args.files):
        try:
            impute_geography(file, args)
        except:
            print("Could not impute geography on {}".format(file))
            continue
