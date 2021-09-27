# usage: python -u src/main_geography_imputer.py --data_path /n/holyscratch01/cga/nicogj/geo_input/ --output_path /n/holyscratch01/cga/nicogj/geo_output/ --host_name holygpu7c26306.rc.fas.harvard.edu --port_number 8874

import argparse
import pandas as pd
import pymapd
import glob
import os
import numpy as np
from tqdm.auto import tqdm
import time

from utils.data_read_in import read_in

def impute_geography(file, args):

    start_time = time.time()

    # Read in Twitter data
    df = read_in(file, args.data_path, cols=['message_id', 'latitude', 'longitude'])
    args.con.load_table("geotweets_{}".format(file.replace('.csv.gz', '')), df, create='infer', method='arrow')

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
            FROM geotweets_{} AS a,
            admin2 AS b
            WHERE ST_Intersects(
                b.omnisci_geo, ST_SetSRID(ST_Point(a.longitude, a.latitude), 4326)
            )
        )
        TO '{}' WITH (delimiter = '\t', quoted = 'true', header='true');
        '''.format(
            file.replace('.csv.gz', ''),
            os.path.join(args.output_path, "geography_{}".format(file.replace('.gz', '')))
        ).replace('\n', ' ')
    )

    args.c.execute(
        '''
        DROP TABLE geotweets_{};
        '''.format(file.replace('.csv.gz', ''))
    )
    del df

    print("Imputed geography for {} in {} min".format(file, round((time.time()-start_time)/60, 1)))

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

    if "adm2" in args.con.get_tables():
        args.c.execute(
            '''
            DROP TABLE IF EXISTS admin2;
            '''
        )
        args.c.execute(
            '''
            CREATE TABLE admin2 AS
            SELECT
                OBJECTID,
                ID_0,
                NAME_0,
                ISO,
                ID_1,
                NAME_1,
                ID_2,
                NAME_2,
                omnisci_geo
            FROM adm2;
            '''.replace('\n', ' ')
        )
        args.c.execute(
            '''
            DROP TABLE adm2;
            '''
        )
    for table in args.con.get_tables():
        if table[:2] == "20":
            args.c.execute(
                '''
                DROP TABLE {};
                '''
            ).format(table)

    if args.filename == '':
        args.files = sorted([os.path.basename(elem) for elem in glob.glob(os.path.join(args.data_path, "*"))])
    else:
        args.files = [args.filename]

    for file in tqdm(args.files):
        try:
            impute_geography(file, args)
        except:
            print("Could not impute geography on {}\n".format(file))
            continue
