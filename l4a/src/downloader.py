import os
import geopandas as gpd

from gedil4a import GediL4a
from shapely.ops import orient


def getAoi( path, names ):

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


# execute main
if __name__ == '__main__':

    # get repo root path
    repo = 'gedi'
    root_path = os.getcwd()[ 0 : os.getcwd().find( repo ) + len ( repo )]

    # get area of interest dataframe
    county_names = ['Kiambu', 'Laikipia', 'Nakuru', 'Nyandarua', 'Nyeri' ]
    aoi = getAoi( os.path.join( root_path, 'aois/kenya' ), county_names )

    # grab meta record of datasets collocated with aoi
    metadata = GediL4a.getGranuleMetadata( aoi )            
    print ( 'meta records: {}'.format( len( metadata ) ) )

    metadata.to_csv( 'granules.csv', columns = ['url'], index=False, header=False )
