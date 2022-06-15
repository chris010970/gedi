import os
import glob
import yaml
import json
import argparse
import numpy as np
import pandas as pd

from munch import munchify
from sentinelhub import parse_time
from sqlalchemy import create_engine

"""
SQL commands
CREATE TABLE kenya.s2_reflectance (LIKE kenya.s2_dumper INCLUDING ALL)
SELECT create_hypertable(  'kenya.s2_reflectance', 'interval_from', if_not_exists => TRUE );
INSERT INTO kenya.s2_reflectance SELECT * FROM kenya.s2_dumper
"""


def writeToDatabase( df ):

    # set shot_number 
    df[ 'shot_number'] = df[ 'shot_number'].astype(np.int64)
    df = df.set_index( 'shot_number' )

    # shorten column names and lowercase
    df.columns = df.columns.str.replace("stats_","")
    df.columns = df.columns.str.lower()

    # convert timeframe to utc datetimes
    df[ 'interval_from' ] = pd.to_datetime( df[ 'interval_from' ], utc=True)
    df[ 'interval_to' ] = pd.to_datetime( df[ 'interval_to' ], utc=True )

    # write to data table
    return df.to_sql(   config.table.name, 
                        engine, 
                        schema=config.schema, 
                        if_exists='append',
                        index=True,
                        index_label='shot_number' )


def convertToDataFrame( data ):
    
    """
    transform response into a pandas.DataFrame
    """

    records = []

    # for all items in response
    for item in data[ 'data' ]:

        entry = {}
        is_valid_entry = True

        # parse data aggregation timeframe
        entry[ 'interval_from' ] = parse_time( item[ 'interval' ][ 'from' ]).date()
        entry[ 'interval_to' ] = parse_time( item[ 'interval' ][ 'to' ]).date()

        for output_name, output_data in item['outputs'].items():
            for band_name, band_values in output_data['bands'].items():

                band_stats = band_values['stats']
                if band_stats['sampleCount'] == band_stats['noDataCount']:
                    is_valid_entry = False
                    break

                # generate unique name
                for stat_name, value in band_stats.items():
                    col_name = f'{output_name}_{band_name}_{stat_name}'
                    if stat_name == 'percentiles':
                        # parse percentile results
                        for perc, perc_val in value.items():
                            perc_col_name = f'{col_name}_{perc}'
                            entry[perc_col_name] = perc_val
                    else:
                        # copy original result
                        entry[col_name] = value

                # response includes histogram analysis
                if band_values.get( 'histogram' ) is not None:

                    # copy raw result
                    col_name = f'{output_name}_{band_name}_histogram'
                    entry[ col_name ] = band_values.get( 'histogram' )

                    # add normalised counts
                    counts = [ value[ 'count' ] for value in entry[ col_name ][ 'bins' ] ]
                    total_counts = sum(counts)
                    
                    entry[ col_name ][ 'normalised_counts' ] = [ round(100 * count / total_counts) if total_counts > 0 else 0 for count in counts ]
                    entry[ col_name ][ 'total_counts' ] = total_counts

                    # add bin edges into array for easy access
                    edges = [ value[ 'lowEdge' ] for value in entry[ col_name ][ 'bins' ] ]
                    edges.append( entry[ col_name ][ 'bins' ][ -1 ][ 'highEdge'] )
                    entry[ col_name ][ 'bin_edges'] = edges


        # append if valid entry
        if is_valid_entry:
            records.append( entry )

    return pd.DataFrame( records )


def getEngine( config ):


    server = config.server
    connection = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format( user=server.user, 
                                                                                    password=server.password, 
                                                                                    host=server.host, 
                                                                                    port=server.port, 
                                                                                    database=server.database )

    return create_engine( connection )


def parseArguments(args=None):

    """
    parse arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='curator')
    parser.add_argument('data_path', action='store', help='data path' )

    return parser.parse_args(args)


# execute main
if __name__ == '__main__':

    # get repo root path
    repo = 'gedi'
    root_path = os.getcwd()[ 0 : os.getcwd().find( repo ) + len ( repo )]
    cfg_path = os.path.join( root_path, 'l4a/cfg' )

    # load config parameters from file
    args = parseArguments()
    with open( os.path.join( cfg_path, 'database/config.yml' ), 'r' ) as f:
        config = munchify( yaml.safe_load( f ) )

    # set schema + table name
    config.schema = 'kenya'
    config.table.name = 's2_bio_dumper'

    # set up database connection engine
    engine = getEngine( config )

    #records = pd.read_sql( 'SELECT DISTINCT (shot_number) FROM kenya.s2_dumper',
    #                        engine )

    subset = pd.DataFrame()

    # get files in data path
    pathnames = glob.glob( os.path.join( args.data_path, '*.json' ), recursive=True )
    for idx, pathname in enumerate( pathnames ):

        # load json file
        with open( pathname, 'r' ) as f:
            obj = json.load( f )

        #if np.int64(  obj[ 'identifier' ] ) not in records[ 'shot_number' ].values:

        # create dataframe
        df = convertToDataFrame( obj[ 'response' ] )
        if len( df ) > 0 and next((True for col in df.columns if 'lai' in col), False):

            df.insert( 0, 'shot_number', obj[ 'identifier' ] )
            subset = pd.concat( [ x for x in [ df, subset ] if not x.empty ], ignore_index=True )

            # concat dataframe to aggregated subset
            if len( subset ) > 20000:

                # write records to database
                writeToDatabase( subset )
                subset = pd.DataFrame()


    # write records to database
    if len( subset ) > 0:
        writeToDatabase( subset )
