import os
import re
import h5py
import requests
import numpy as np
import pandas as pd
import geopandas as gpd

from datetime import datetime
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon


class GediL4a():

    # constants
    doi = '10.3334/ORNLDAAC/2056'
    base_url = 'https://cmr.earthdata.nasa.gov/search/'
    page_size = 2000


    def __init__( self, pathname ):

        """
        constructor
        """

        # open file as read-only
        self._hf = h5py.File( pathname, 'r' )

        # base datetime to convert l4a delta times
        self._base_time = datetime(2018, 1, 1, 0, 0, 0)
        return


    def getBeamData( self, aoi=None ):

        """
        getBeamData
        """

        # empty dataframe
        df = pd.DataFrame()

        # scan through keys
        for key in list( self._hf.keys()):
            if key.startswith( 'BEAM' ):

                # get beam group
                group = self._hf.get( key )
                beam = self.getGroupData( group )

                # merge in land cover data
                beam = pd.merge( beam, 
                                self.getGroupData( group[ 'land_cover_data'] ), 
                                how='inner', 
                                on='shot_number' )

                # reject null retrievals
                beam = beam.replace( -9999, np.nan )    
                beam = beam[ beam[ 'agbd' ].notna() ]

                # apply basic qa filtering
                beam = beam[ beam[ 'algorithm_run_flag' ] > 0 ]
                beam = beam[( beam[ 'l2_quality_flag' ] == 1) & ( beam[ 'l4_quality_flag' ] == 1 ) ]

                # create datetime column
                beam[ 'datetime' ] = self._base_time + pd.to_timedelta( beam.delta_time, unit='s' )

                # drop superfluous columns
                beam = beam.drop( [ 'algorithm_run_flag', 'l2_quality_flag', 'l4_quality_flag' ], axis=1 )
                df = pd.concat( [ x for x in [ df, beam ] if not x.empty ], ignore_index=True )

        # convert to a geopandas dataframe
        gdf = gpd.GeoDataFrame( df, geometry=gpd.points_from_xy( df.lon_lowestmode, df.lat_lowestmode ) )
        gdf = gdf.drop( [ 'lon_lowestmode', 'lat_lowestmode' ], axis=1 )
        gdf = gdf.set_crs( 'EPSG:4326' )

        # filter on aoi
        if aoi is not None:
            gdf = gdf[ gdf['geometry'].within( aoi ) ]

        return gdf


    def getGroupData( self, group ):
        
        """
        getGroupData
        """

        # iterate through values
        names, values = [], []
        for key, value in group.items():
                    
            if not isinstance( value, h5py.Group ):
                
                # 1d vars
                if ( len(value.shape) == 1 ):
                    names.append( key )
                    values.append( value[:].tolist() )
                else:
                    # handling for 2d covariance matrices
                    if ( len(value.shape) == 2 ):
                        for idx in range( value.shape[1] ):
                            names.append( key + '_' + str( idx + 1 ) )
                            values.append( value[:, idx].tolist() )
                    else:
                        # ignore 3d params for now
                        continue

        return pd.DataFrame( map(list, zip(*values)), columns=names )


    def getGeolocationData( self, aoi=None ):

        """
        getGeolocationData
        """

        # intermediate lists
        lat_l, lon_l, time_l, beam_n, shot_l = [], [], [], [], []

        # scan through keys
        for key in list( self._hf.keys()):
            if key.startswith('BEAM'):

                # get beam group
                beam = self._hf.get( key )

                # retrieve coords and times
                lat = beam.get('lat_lowestmode')[:]
                lon = beam.get('lon_lowestmode')[:]
                time = beam.get('delta_time')[:] 
                shot = beam.get('shot_number')[:] 

                # add to current list
                lat_l.extend(lat.tolist())
                lon_l.extend(lon.tolist())
                time_l.extend(time.tolist()) 
                shot_l.extend(shot.tolist()) 

                # number of shots in the beam group
                n = lat.shape[0] 
                beam_n.extend( np.repeat( str(key), n).tolist() )
            
        # create dataframe with datetime column
        df = pd.DataFrame( list(zip(shot_l,beam_n,lat_l,lon_l,time_l) ), columns=['shot_number', 'beam', 'lat', 'lon', 'delta_time' ] )
        df[ 'datetime' ] = self._base_time + pd.to_timedelta( df.delta_time, unit='s' )
        df[ 'datetime' ] = df[ 'datetime' ].tz_localize('UTC')

        # convert to geodataframe registered to geographic crs
        gdf = gpd.GeoDataFrame(  df, geometry=gpd.points_from_xy( df.lon, df.lat ) )
        gdf.set_crs( 'EPSG:4326' )

        # turn fill values (-9999) to nan
        gdf = gdf.drop( [ 'lat', 'lon', 'delta_time' ], axis=1 )
        gdf = gdf.replace( -9999, np.nan )    

        # filter on aoi
        if aoi is not None:
            gdf = gdf[ gdf['geometry'].within( aoi ) ]

        return gdf


    @staticmethod
    def getGranuleMetadata( aoi ):

        """
        getGranuleMetadata
        """

        def getCollectionConceptId():

            """
            getCollectionConceptId
            """

            # retrieve metadata catalogue
            doisearch = GediL4a.base_url + f'collections.json?doi={GediL4a.doi}'
            return requests.get(doisearch).json()['feed']['entry'][0]['id']

        # initialise vars
        metadata = list()    
        page_num = 1

        # loop until break
        while True:
        
            # define search and payload parameters
            data = {
                "collection_concept_id": getCollectionConceptId(), 
                "page_size": GediL4a.page_size,
                "page_num": page_num,
                "simplify-shapefile": 'true' # required to bypass 5000 coordinates limit of CMR
            }

            payload = { "shapefile": ( "search.json", aoi.geometry.to_json(), "application/geo+json" ) }

            # execute post search        
            search = GediL4a.base_url + 'granules.json'
            response = requests.post( search, data=data, files=payload)
            granules = response.json()['feed']['entry']
        
            # granules in response ?
            if granules:
                
                # iterate through granules
                for g in granules:
                                    
                    # capture info in dictionary
                    info = { 'url' : '', 'geometry' : '', 'size' : float( g['granule_size'] ) }
                    if 'polygons' in g:

                        # split multifeature list into separate objects
                        polygons = []
                        for polygon in g['polygons']:
                            i=iter(polygon[0].split(" "))

                            # append to list
                            ltln = list(map(" ".join,zip(i,i)))
                            polygons.append(Polygon([[float(p.split(" ")[1]), float(p.split(" ")[0])] for p in ltln]))

                        # combine polygons into single object
                        info[ 'geometry' ] = MultiPolygon( polygons )

                    # get dataset url
                    for links in g['links']:
                        if 'title' in links and links['title'].startswith('Download') and links['title'].endswith('.h5'):
                            info[ 'url' ] = links['href']

                    # add to list        
                    metadata.append( info )
                
                # next page
                page_num += 1
            
            else: 
                # end of granule search
                break
            
        # construct metadata geodataframe registered to geographic crs
        gdf = gpd.GeoDataFrame( metadata )
        gdf = gdf.set_crs( "EPSG:4326" )

        # drop duplicates + fix urls
        gdf = gdf.drop_duplicates( subset=['url'] )
        gdf['url'] = gdf['url'].astype( str )

        # get acquisition datetimes from urls
        gdf[ 'acqtime' ] = GediL4a.getAcquisitionTimes( list( gdf[ 'url' ].values ) )
        return gdf

    @staticmethod
    def getAcquisitionTimes( pathnames ):

        acq_times = []

        # convert to list if required
        if not isinstance( pathnames, list ):
            pathnames = [ pathnames ]

        # for each pathname / url
        for pathname in pathnames:

            # extract 13 digit delimited by underscores
            m = re.search( '_[0-9]{13}_', os.path.basename( pathname ) )
            dt = str( m.group(0) )

            # convert year / julian date to datetime
            dt = re.sub('[^0-9]','', dt)
            acq_times.append( datetime.strptime( dt, '%Y%j%H%M%S') )

        return acq_times

