import os
import yaml
import glob
import argparse
import geopandas as gpd

from munch import munchify
from shapely.ops import orient
from sqlalchemy import create_engine

from gedil4a import GediL4a


def getAoi( path, names ):

    """
    getAoi
    """

    # load selected county boundaries
    def getCounties( names ):         
        return admin[ admin [ 'Name' ].isin( names ) ]

    # load kenyan admin boundaries
    admin = gpd.read_file( os.path.join( path, 'admin.shp' ) ) 
    counties = getCounties( names )

    # expand before dissolve to ensure boundaries overlap
    counties['geometry'] = counties['geometry'].buffer(0.001)
    counties = counties.dissolve( aggfunc='sum')

    # orient polygon points clockwise
    counties.geometry = counties.geometry.apply( orient, args=(1,) )
    return counties


def writeToDataTable( pathname, aoi, config ):

    """
    writeToDataTable
    """

    # get beam data
    obj = GediL4a( pathname )
    gdf = obj.getBeamData( aoi=aoi.geometry.iloc[ 0 ] )
    gdf = gdf.set_index( 'shot_number' )

    # set up database connection engine
    server = config.server
    connection = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format( user=server.user, 
                                                                                    password=server.password, 
                                                                                    host=server.host, 
                                                                                    port=server.port, 
                                                                                    database=server.database )
    engine = create_engine( connection )

    # geoDataFrame to postGIS - append to existing table
    gdf.to_postgis( con=engine,
                    name=config.table.name,
                    schema=config.table.schema,
                    if_exists='append', 
                    index=True )

    return 


def parseArguments(args=None):

    """
    parse arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='ingestor')

    # mandatory args
    parser.add_argument('data_path', action='store', help='path to level-4a datasets' )
    parser.add_argument('config_file', action='store', help='yaml configuration file' )

    return parser.parse_args(args)


# execute main
if __name__ == '__main__':

    # get repo root path
    repo = 'gedi'
    root_path = os.getcwd()[ 0 : os.getcwd().find( repo ) + len ( repo )]

    # get area of interest dataframe
    county_names = ['Kiambu', 'Laikipia', 'Nakuru', 'Nyandarua', 'Nyeri' ]
    aoi = getAoi( os.path.join( root_path, 'aois/kenya' ), county_names )

    # load config parameters from file
    args = parseArguments()
    with open( args.config_file, 'r' ) as f:
        config = munchify( yaml.safe_load( f ) )

    # write datasets to postgis data table
    pathnames = glob.glob( '{path}\\*.h5'.format( path=args.data_path ) ) 
    for pathname in pathnames:
        writeToDataTable( pathname, aoi, config )
