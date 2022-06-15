import os
import json
import yaml
import argparse
import pandas as pd
import geopandas as gpd

from munch import munchify
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from client import Client


def getRequestId( pathname ):

    request_id = None

    # response json exists
    if os.path.exists( pathname ):

        # save response to file
        with open( pathname, 'r' ) as f:
            response = json.load( f )

        # extract id
        request_id = response.get( 'id' )
            
    return request_id


def getRequests( records, client, args ):

    request_ids = []
    for offset in range ( 0, len( records ), args.chunk_size ):

        # process records in chunks - transform to local utm
        subset = records[ offset : offset + args.chunk_size ].copy()
        subset = subset.to_crs( subset.estimate_utm_crs() )
        subset.geometry = subset.geometry.buffer( 30 )

        # create unique pathname to save geodatabase
        date = pd.to_datetime( row.date ).strftime('%Y%m%d')
        path = os.path.join( args.out_path, f'{date}_{offset}' )
        
        # check if api request for record subset already created 
        request_id = getRequestId( os.path.join( path, 'response.json' ) )
        if request_id is None:

            # save batch api compatible geodatabase file to disc
            pathname = os.path.join( path, 'polygons.gpkg' )
            if ( getGeoDatabase( subset, pathname ) ):
            
                args.timeframe = { 'start' : row.date - delta, 'end' : row.date + delta }
                
                # aws related info
                aws = munchify( { 'bucket' : args.bucket, 'prefix' : os.path.join( args.prefix, f'{date}_{offset}' ) } )
                aws.prefix = aws.prefix.replace(os.sep, '/' )

                # post request
                status_code, response = client.postRequest( pathname, aws, args )
                if status_code == 201:            

                    # save response to file
                    with open( os.path.join( path, 'response.json' ), 'w', encoding='utf-8' ) as f:
                        json.dump( response, f, ensure_ascii=False, indent=4)

                    request_id = response[ 'id' ]

        # append valid request id to list
        if request_id is not None:
            request_ids.append( request_id )

    return request_ids



def getGeoDatabase( subset, pathname ):

    subset = subset [ [ 'shot_number', 'geometry' ] ]
    geodb = subset.copy()

    # rename columns for batch api
    geodb['id'] = geodb.reset_index().index
    geodb = geodb.rename( columns={ 'shot_number': 'identifier' } )
    geodb[ 'identifier'] = geodb[ 'identifier'].astype(str)

    # create folder if not exists
    if not os.path.exists( os.path.dirname( pathname ) ):
        os.makedirs( os.path.dirname( pathname ) )

    # write to file as geodatabase 
    geodb.to_file( pathname, driver='GPKG', index=False )
    return os.path.exists( pathname )


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
                        AND landsat_treecover >= {min_treecover}
                """ .format(    schema=config.table.schema, 
                                table=config.table.name,
                                date=date,
                                min_agbd=args.min_agbd,
                                max_agbd=args.max_agbd,
                                min_treecover=args.min_treecover )

    return gpd.GeoDataFrame.from_postgis(   command, 
                                            create_engine( connection ), 
                                            geom_col='geometry' )


def getClient( pathname ):

    with open( pathname, 'r' ) as f:
        config = munchify( yaml.safe_load( f ) )

    return Client( config )


def parseArguments(args=None):

    """
    parse arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='curator')
    parser.add_argument('bucket', action='store', help='api pathname' )
    parser.add_argument('prefix', action='store', help='api pathname' )
    parser.add_argument('out_path', action='store', help='api pathname' )

    # optional args
    parser.add_argument('--delta', type=int, help='timeframe delta', default=504 )
    parser.add_argument('--chunk_size', type=int, help='chunk size', default=10000 )

    parser.add_argument('--min_agbd', type=int, help='min aboveground biomass', default=5 )
    parser.add_argument('--max_agbd', type=int, help='max aboveground biomass', default=10000 )
    parser.add_argument('--min_treecover', type=int, help='min treecover', default=5 )

    parser.add_argument('--resolution', type=int, help='timeframe delta', default=10 )
    parser.add_argument('--interval', type=str, help='timeframe delta', default='P1D' )


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

    # get gedi timestamps
    timestamps = getTimestamps( config )
    timestamps[ 'date' ] = pd.to_datetime( timestamps[ 'date'] )    

    # get sentinel-hub client
    client = getClient( os.path.join( cfg_path, 'sentinelhub/config.yml' ) )

    # iterate through unique timestamps
    delta = timedelta(hours=args.delta)
    for idx, row in timestamps.iterrows():

        # get request ids - from file / server
        request_ids = getRequests( getRecords( config, row.date, args ), client, args )
        for request_id in request_ids:
            
            # get current status of api request
            response = client.getStatus( request_id )

            # request created -> analyze
            if ( response[ 'status' ] == 'CREATED' ):

                status_code = client.setAnalysis( request_id )
                print( f'Set Analysis: {request_id} -> {status_code}' )

            # analysis complete -> start
            if ( response[ 'status' ] == 'ANALYSIS_DONE' ):

                status_code = client.startRequest( request_id )
                print( f'Start Request: {request_id} -> {status_code}' )

            # processing request
            if ( response[ 'status' ] == 'PROCESSING' ):
                completionPercentage = response[ 'completionPercentage' ]
                print( f'Processing Request: {request_id} - completed {completionPercentage}%' )

            # processing completed
            if ( response[ 'status' ] == 'DONE' ):
                print( f'Request completed: {request_id}' )

        if idx == 150:
            break
