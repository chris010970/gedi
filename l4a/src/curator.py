import os
import yaml
import argparse
import pandas as pd
import geopandas as gpd

from munch import munchify
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from sentinelhub import CRS
from statisticalapi import Client


def getMetrics( df, args ):

    # create sentinelhub statistical api client
    with open( os.path.join( args.cfg_path, 's2-metrics.yml' ), 'r' ) as f:
        client = Client( munchify( yaml.safe_load( f ) ) )
     
    # define 30m buffer around lidar point observations
    df.geometry = df.geometry.buffer( 20 )

    try:
        # retrieve statistics for cloudfree geometries
        response = client.getStatistics( [ args.timeframe ], 
                                        resolution=10,
                                        polygons=df, 
                                        interval='P1D' )
    except BaseException as e:
        print ( 'Exception error: {error}'.format( error=str ( e ) ) )
        response = None

    return response


def filterClearScenes( clear_scenes, args ):

    # iterate through id / shot_number
    samples = list()
    for _id in clear_scenes.id.unique():

        # select clearfree scenes closest to acquisition date
        df = clear_scenes.loc[ clear_scenes[ 'id' ] == _id ]
        samples.append( df.sort_values( 'delta' )[ 0 : args.max_scenes ] )

    # reconstitute filtered cloudfree scenes
    return pd.concat( samples, ignore_index=True ) 


def getClearScenes( subset, args ):

    # create sentinelhub statistical api client
    with open( os.path.join( args.cfg_path, 's2-cloud-cover.yml' ), 'r' ) as f:
        client = Client( munchify( yaml.safe_load( f ) ) )

    # determine s2cloudfree output for 200m polygon buffer around lidar points
    aoi = gpd.GeoDataFrame().assign( id=subset['shot_number'], geometry=subset['geometry'] )
    aoi.geometry = aoi.geometry.buffer( 200 )

    # default to empty
    try:

        # retrieve cloud mask statistics
        response =  client.getStatistics( [ args.timeframe ], 
                                    resolution=160,
                                    polygons=aoi, 
                                    interval='P1D' )

        # concat and drop cloudy dataframes 
        df = pd.concat( [ x for x in response._dfs if not x.empty ], ignore_index=True )
        df = df.drop( df[ df.data_B0_mean > 0.1 ].index )

        # merge to align geometry
        df = df [ [ 'id', 'interval_from', 'interval_to' ] ]
        pd.merge( subset, df, how='inner', left_on = 'shot_number', right_on='id')

    except BaseException as e:
        print ( 'Exception error: {error}'.format( error=str ( e ) ) )
        df = pd.DataFrame()

    return df


def getConnectionString( config ):

    server = config.server
    return 'postgresql://{user}:{password}@{host}:{port}/{database}'.format( user=server.user, 
                                                                            password=server.password, 
                                                                            host=server.host, 
                                                                            port=server.port, 
                                                                            database=server.database )


def getTimestamps( config ):

    # set up database connection engine
    command = "SELECT DISTINCT DATE (datetime ) FROM {schema}.{table}".format( schema=config.table.schema, 
                                                                                table=config.table.name )

    return pd.read_sql( command, create_engine( getConnectionString( config ) ) )


def getRecords( config, date, args ):

    # set up database connection engine
    server = config.server
    connection = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format( user=server.user, 
                                                                                    password=server.password, 
                                                                                    host=server.host, 
                                                                                    port=server.port, 
                                                                                    database=server.database )
    # create and execute command
    command = """ \
              SELECT * FROM {schema}.{table} \
                WHERE DATE(datetime) = '{date}' \
                    AND agbd >= {min_agbd} AND agbd <= {max_agbd} \
                        AND landsat_treecover >= {min_treecover} \
                            ORDER BY RANDOM()
                                LIMIT {max_records}
                """ .format(    schema=config.table.schema, 
                                table=config.table.name,
                                date=date,
                                min_agbd=args.min_agbd,
                                max_agbd=args.max_agbd,
                                min_treecover=args.min_treecover,
                                max_records=args.max_records )

    return gpd.GeoDataFrame.from_postgis(   command, 
                                            create_engine( connection ), 
                                            geom_col='geometry' )


def parseArguments(args=None):

    """
    parse arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='curator')
    parser.add_argument('cfg_path', action='store', help='configuration path' )
    parser.add_argument('db_file', action='store', help='database configuration file' )
    parser.add_argument('out_path', action='store', help='output path' )

    # optional args
    parser.add_argument('--delta', type=int, help='timeframe delta', default=504 )
    parser.add_argument('--chunk_size', type=int, help='chunk size', default=100 )
    parser.add_argument('--min_agbd', type=int, help='min aboveground biomass', default=20 )
    parser.add_argument('--max_agbd', type=int, help='max aboveground biomass', default=10000 )
    parser.add_argument('--min_treecover', type=int, help='min treecover', default=70 )
    parser.add_argument('--max_records', type=int, help='max records', default=600 )
    parser.add_argument('--max_scenes', type=int, help='max records', default=3 )
    parser.add_argument('--min_date', type=str, help='min date', default=datetime.strptime('2018-01-01', '%Y-%m-%d' ) )
    parser.add_argument('--max_date', type=str, help='max date', default=datetime.strptime('2020-31-12', '%Y-%m-%d' ) )

    return parser.parse_args(args)


# execute main
if __name__ == '__main__':

    # get repo root path
    repo = 'gedi'
    root_path = os.getcwd()[ 0 : os.getcwd().find( repo ) + len ( repo )]
    min_scenes_per_request = 10

    # load config parameters from file
    args = parseArguments()
    with open( args.db_file, 'r' ) as f:
        db_config = munchify( yaml.safe_load( f ) )

    # create output directory
    if not os.path.exists( args.out_path ):
        os.makedirs( args.out_path )

    # get gedi timestamps
    timestamps = getTimestamps( db_config )
    timestamps[ 'date' ] = pd.to_datetime( timestamps[ 'date'] )    
    timestamps = timestamps[ ( timestamps.date >= args.min_date ) & ( timestamps.date <= args.max_date ) ]

    # iterate through unique timestamps
    delta = timedelta(hours=args.delta)
    for idx, row in timestamps.iterrows():

        # construct output pathname 
        filename = 'stats_{date}_{min_agbd}_{max_agbd}.csv'.format( min_agbd=args.min_agbd, max_agbd=args.max_agbd, date=row.date.strftime( '%Y-%m-%d.csv' ) )
        pathname = os.path.join( args.out_path, filename )

        # ignore if exists
        if not os.path.exists( pathname ):

            # get random selection of gedi records with matching timestamp
            samples = list()
            records = getRecords( db_config, row.date, args )

            print( 'processing date: {date} ({idx} / {total}) - records {records}'.format( date=row.date.strftime('%Y-%m-%d'), 
                                                                                            idx=idx + 1, 
                                                                                            total=len(timestamps),
                                                                                            records=len( records ) ) )
            for offset in range ( 0, len( records ), args.chunk_size ):

                # process records in chunks - transform to local utm
                subset = records[ offset : offset + args.chunk_size ].copy()
                subset = subset.to_crs( subset.estimate_utm_crs() )

                # get cloudless statistics collocated with gedi observations 
                args.timeframe = { 'start' : row.date - delta, 'end' : row.date + delta }
                clear_scenes = getClearScenes( subset, args )
                
                # pick cloudfree scenes closest to acquisition date
                clear_scenes[ 'delta'] = abs ( ( clear_scenes[ 'interval_from' ] - row.date.date() ).dt.days )
                clear_scenes = filterClearScenes( clear_scenes, args )

                # check for empty dataframe
                if not clear_scenes.empty:

                    # iterate through unique clear scene datetimes
                    for timestamp in clear_scenes.interval_from.unique():

                        # extract cloud-free geometries for datetime
                        args.timeframe = { 'start' : timestamp, 'end' : timestamp + timedelta(hours=24) }
                        df = clear_scenes[ clear_scenes.interval_from == timestamp ]

                        if len( df ) > min_scenes_per_request:

                            # merge with lidar subset - convert to geodataframe 
                            gdf = gpd.GeoDataFrame( pd.merge( df, 
                                                            subset[ [ 'shot_number', 'geometry' ] ], 
                                                            how='inner', 
                                                            left_on = 'id', 
                                                            right_on='shot_number' ) )

                            print( '... scene: {timestamp} - number of cloudfree geometries: {count}' \
                                .format ( timestamp=timestamp.strftime( '%Y-%m-%d'), count=len( gdf ) ) )

                            # retrieve reflectance / vi statistics - append to list
                            response = getMetrics( gdf, args )

                            # append concatenated frame - check for empties
                            if ( len( [ x for x in response._dfs if not x.empty ] ) > 0 ):
                                samples.append( pd.concat( [ x for x in response._dfs if not x.empty ], ignore_index=True ) )


            # merge sample stats with gedi dataframe
            stats = pd.merge( records, 
                            pd.concat( samples, ignore_index=True ), 
                            how='inner', 
                            left_on = 'shot_number', 
                            right_on='id' )

            # drop superfluous columns and sort on shot number
            stats = stats.drop( [ 'id', 'interval_from', 'interval_to' ], axis=1 )
            stats = stats.sort_values( 'shot_number' )

            # save to csv file            
            stats.to_csv( pathname )
            print( f'... created file: {pathname}' )
