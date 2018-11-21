import gzip
import requests
import re
import numpy as np
import pandas as pd
import datetime as dt
from pathlib import Path
import urllib3
import wutils

class GSOD(object):
    '''
    Simple python API-like. Download GSOD weather data from NOAA servers.
    '''

    def __init__(self):
        pass


    @staticmethod
    def stationSearch(selection):
        '''
        Parameters
        ----------
        selection : dict, keys: 'ctry', 'station_name', 'state'
        e.g. {'ctry': 'UK'}, {'state': 'IA'}, {'station_name': 'STANTON'}
        '''
        try:
            print('Preparing isd-history.csv ...')
            url =  'http://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'
            #url = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'			      
            ''' HTTP Error 503: Service Unavailable
            # error message at work despite service
            # working properly
            '''
            #url = 'https://s3.amazonaws.com/aws-gsod/isd-history.csv'
            df_mapping = {'USAF' : str,
                            'WBAN' : str,
                            'STATION NAME' : str,
                            'CTRY' : str,
                            'STATE' : str,
                            'ICAO' : str,
                            'LAT' : float,
                            'LON' : float,
                            'ELEV(M)' : float,
                            'BEGIN' : str,
                          'END' : str}
            date_parser = ['BEGIN', 'END']

            cached_file = Path('isd-history.csv')
                                    
            if not cached_file.is_file():   
                print(url)             
                http = urllib3.PoolManager()
                r = http.request('GET', url, preload_content=False)                
                with open('isd-history.csv', 'wb') as out:
                    while True:
                        chunk_size = 1024
                        data = r.read(chunk_size)
                        if not data:
                            break
                        out.write(data)                
                r.release_conn()             
                print('downloaded from NOAA servers.')
            else:
                print('using locally cached file.')
            
            isd_hist = pd.read_csv(cached_file,
                                    dtype=df_mapping,
                                    parse_dates=date_parser)

            # print('Download complete! {}'.format(isd_hist.shape))
            
            # Rename 'STATION NAME' to 'STATION_NAME'
            isd_hist = isd_hist.rename(index=str, columns={'STATION NAME' : 'STATION_NAME'})

            # Merge 'USAF' and 'WBAN'
            isd_hist['station_id'] = isd_hist.USAF + '-' + isd_hist.WBAN
            
            # Get rid of useless columns
            isd_hist = isd_hist.drop(['USAF', 'WBAN', 'ICAO', 'ELEV(M)'], axis=1)
         
            # Headers to lower case
            isd_hist.columns = isd_hist.columns.str.lower()
            
                        
            acc = []
            for k, v in selection.items():
                if k == 'begin':
                    sign = '<'
                elif k == 'end':
                    sign = '>'
                else:
                    sign = '='

                if isinstance(v, list):
                    acc.append('{} '.format(k) + sign + '= {} & '.format(v))
                else:
                    acc.append('{} '.format(k) + sign + '= {} & '.format(''.join(v)))
                
            # this qury does not work any more, quick fix just to make it go for a country: 
            # return isd_hist.query(re.sub('(?=.*)&.$','' ,''.join(acc)))
            return isd_hist.query("ctry == '{}'".format(selection['ctry']))
            
        except Exception as e:
            print(e)

    @staticmethod
    def getData(station=None, start=dt.datetime.now().year, end=dt.datetime.now().year, metric=True, **kwargs):
        '''
        Get weather data from the internet as memory stream
        '''
        big_df = pd.DataFrame()

        for year in range(start, end+1):
            
            cached_path = str(station) + '-99999-' + str(year) + '.op.gz'
            cached_file = Path(cached_path)
                                    
            if not cached_file.is_file():   
                # Define URL
                url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/' + str(year) + '/' + str(station) \
                + '-99999-' + str(year) + '.op.gz'
                print(url)             
                http = urllib3.PoolManager()
                r = http.request('GET', url, preload_content=False)                
                with open(cached_path, 'wb') as out:
                    while True:
                        chunk_size = 1024
                        data = r.read(chunk_size)
                        if not data:
                            break
                        out.write(data)                
                r.release_conn()             
                print('Downloaded {} from NOAA servers.'.format(cached_path))
            else:
                print('Using locally cached file {}'.format(cached_path))

            # Define data stream
            #stream = requests.get(url)

            # Unzip on-the-fly
            #decomp_bytes = gzip.decompress(stream.content)
            
            in_file = open(cached_file, "rb") # opening for [r]eading as [b]inary
            decomp_bytes = gzip.decompress(in_file.read())
            data = decomp_bytes.decode('utf-8').split('\n')
            in_file.close()
            '''
            Data manipulations and ordering
            '''
            # Remove start and end
            data.pop(0) # Remove first line header
            data.pop()  # Remove last element

            # Define lists
            (stn, wban, date, temp, temp_c, dewp, dewp_c,
             slp, slp_c, stp, stp_c, visib, visib_c,
             wdsp, wdsp_c, mxspd, gust, max, max_f, min, min_f,
             prcp, prcp_f, sndp, f, r, s, h, th, tr) = ([] for i in range(30))

            # Fill in lists
            for i in range(0, len(data)):
                stn.append(data[i][0:6])
                wban.append(data[i][7:12])
                date.append(data[i][14:22])         
                temp.append(data[i][25:30])
                temp_c.append(data[i][31:33])
                dewp.append(data[i][36:41])
                dewp_c.append(data[i][42:44])
                slp.append(data[i][46:52])      # Mean sea level pressure
                slp_c.append(data[i][53:55])
                stp.append(data[i][57:63])      # Mean station pressure
                stp_c.append(data[i][64:66])
                visib.append(data[i][68:73])
                visib_c.append(data[i][74:76])
                wdsp.append(data[i][78:83])
                wdsp_c.append(data[i][84:86])
                mxspd.append(data[i][88:93])
                gust.append(data[i][95:100])
                max.append(data[i][103:108])
                max_f.append(data[i][108])
                min.append(data[i][111:116])
                min_f.append(data[i][116])
                prcp.append(data[i][118:123])
                prcp_f.append(data[i][123])
                sndp.append(data[i][125:130])   # Snow depth in inches to tenth
                f.append(data[i][132])          # Fog
                r.append(data[i][133])          # Rain or drizzle
                s.append(data[i][134])          # Snow or ice pallet
                h.append(data[i][135])          # Hail
                th.append(data[i][136])         # Thunder
                tr.append(data[i][137])         # Tornado or funnel cloud

            '''
            Replacements
            min_f & max_f
            blank   : explicit => e
            *       : derived => d
            '''
            max_f = [re.sub(pattern=' ', repl='e', string=x) for x in max_f] # List comprenhension
            max_f = [re.sub(pattern='\*', repl='d', string=x) for x in max_f]

            min_f = [re.sub(pattern=' ', repl='e', string=x) for x in min_f]
            min_f = [re.sub(pattern='\*', repl='d', string=x) for x in min_f]

            '''
            Create dataframe & cleanse data
            '''
            # Create intermediate matrix
            mat = np.matrix(data=[stn, wban, date, temp, temp_c, dewp, dewp_c,
                   slp, slp_c, stp, stp_c, visib, visib_c,
                   wdsp, wdsp_c, mxspd, gust, max, max_f, min, min_f,
                   prcp, prcp_f, sndp, f, r, s, h, th, tr]).T

            # Define header names
            headers = ['stn', 'wban', 'date', 'temp', 'temp_c', 'dewp', 'dewp_c',
            'slp', 'slp_c', 'stp', 'stp_c', 'visib', 'visib_c',
            'wdsp', 'wdsp_c', 'mxspd', 'gust', 'maxtemp', 'max_f', 'mintemp', 'min_f',
            'prcp', 'prcp_f', 'sndp', 'f', 'r', 's', 'h', 'th', 'tr']

            # Set precision
            pd.set_option('precision', 3)

            # Create dataframe from matrix object
            df = pd.DataFrame(data=mat, columns=headers)

            # Replace missing values with NAs
            df = df.where(df != ' ', 9999.9)

            # Create station ids
            df['station_id'] = df['stn'].map(str) + '-' + df['wban'].map(str)
            df = df.drop(['stn', 'wban'], axis=1)

            # Convert to numeric
            df[['temp', 'temp_c', 'dewp', 'dewp_c', 'slp', 'slp_c',
                'stp', 'stp_c', 'visib', 'visib_c', 'wdsp', 'wdsp_c',
                'mxspd',  'gust', 'maxtemp', 'mintemp', 'prcp', 'sndp']] = df[['temp', 'temp_c', 'dewp',
                                                                       'dewp_c', 'slp', 'slp_c', 'stp',
                                                                       'stp_c', 'visib', 'visib_c', 'wdsp',
                                                                       'wdsp_c', 'mxspd', 'gust', 'maxtemp',
                                                                       'mintemp', 'prcp', 'sndp']].apply(pd.to_numeric, errors='raise')
            
            if(metric):
                '''
                Convert to metric
                '''
                print('Converting data to metric.')
                df.temp = df.temp.apply(wutils.fahrenheitToCelzius)                                
                df.mintemp = df.mintemp.apply(wutils.fahrenheitToCelzius)
                df.maxtemp = df.maxtemp.apply(wutils.fahrenheitToCelzius)
                df.dewp = wutils.fahrenheitToCelzius(df.dewp)
                df.visib = wutils.milesToKm(df.visib)
                df.wdsp = wutils.knotsToKm(df.wdsp)
                df.mxspd = wutils.knotsToKm(df.mxspd)
                df.gust = wutils.knotsToKm(df.gust)
                df.prcp = wutils.inchToCm(df.prcp)
                df.sndp = wutils.inchToCm(df.sndp)
                
            # Replace missing weather data with NaNs
            df = df.replace(to_replace=[99.99, 99.9,999.9,9999.9], value=np.nan)
            
            # Convert to date format
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

            big_df = pd.concat([big_df, df])
            
            # Add dayofyear and dayofweek
            big_df['dayofyear'] = big_df.date.dt.dayofyear
            big_df['dayofweek'] = big_df.date.dt.dayofweek  
            
            big_df = big_df.groupby([big_df.dayofyear], as_index=False).agg({'mintemp' : np.min, 
                                                             'maxtemp' : np.max, 
                                                             'prcp':np.max, 
                                                             'visib':np.max, 
                                                             'wdsp':np.max,
                                                             'dayofyear':np.max,
                                                             'dayofweek':np.max,
                                                             'f':np.max,
                                                             'r':np.max,
                                                             's':np.max,
                                                             'h':np.max,
                                                             'th':np.max,
                                                             'tr':np.max})

            print('station: {}\year {}\tDone!'.format(str(station), str(year)))

        return big_df
