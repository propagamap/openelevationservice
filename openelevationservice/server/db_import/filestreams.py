# -*- coding: utf-8 -*-

from openelevationservice import TILES_DIR, SETTINGS
from openelevationservice.server.utils.logger import get_logger

from os import path, environ
import requests
import subprocess
import zipfile
from bs4 import BeautifulSoup

try:
    from io import BytesIO
except:
    from StringIO import StringIO
    
log = get_logger(__name__)

def downloadsrtm(xy_range):
    """
    Downlaods SRTM v4.1 tiles2 as bytestream and saves them to TILES_DIR.
    
    :param xy_range: The range of tiles2 in x and y as per grid in
        http://srtm.csi.cgiar.org/SELECTION/inputCoord.asp
        in 'minx, maxx, miny, maxy.
    :type xy_range: comma-separated range string
    """
    
    base_url = r'https://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/'
    
    # Create session for authentication
    session = requests.Session()
    
    # pw = environ.get('SRTMPASS')
    # user = environ.get('SRTMUSER')
    # if not user and not pw:
    #     auth = tuple(SETTINGS['srtm_parameters'].values())
    # else:
    #     auth = tuple([user,pw])
    # session.auth = auth
    
    # log.debug("SRTM credentials: {}".format(session.auth))

    for x in range(*xy_range[0]):
        x_fill = str(x).zfill(2)
        for y in range(*xy_range[1]):
            y_fill = str(y).zfill(2)
            tile_basename = 'srtm_{}_{}'.format(x_fill, y_fill)
            if not path.exists(path.join(TILES_DIR, tile_basename + '.tif')):
                response = session.get(base_url + tile_basename + '.zip')
                if response.status_code == 200:
                    with zipfile.ZipFile(BytesIO(response.content)) as zip_obj:
                        # Loop through the files in the zip
                        for filename in zip_obj.namelist():
                            # Don't extract the readme.txt
                            if filename != 'readme.txt':
                                data = zip_obj.read(filename)
                                # Write byte contents to file
                                with open(path.join(TILES_DIR, filename), 'wb') as f:
                                    f.write(data)
                    log.info('Downloaded {}'.format(filename))
            else:
                log.debug("File {}.tif already exists in {}".format(tile_basename, TILES_DIR))


def raster2pgsql():
    """
    Imports SRTM v4.1 tiles to PostGIS.
    
    :raises subprocess.CalledProcessError: Raised when raster2pgsql throws an error.
    """
    
    pg_settings = SETTINGS['provider_parameters']
    
    # Copy all env variables and add PGPASSWORD
    env_current = environ.copy()
    env_current['PGPASSWORD'] = pg_settings['password']
    
    # Tried to import every raster individually by user-specified xyrange 
    # similar to download(), but raster2pgsql fuck it up somehow.. The PostGIS
    # raster will not be what one would expect. Instead doing a bulk import of all files.
    cmd_raster2pgsql = r"raster2pgsql -s 4326 -a -C -M -P -t 50x50 {filename} {table_name} | psql -q -h {host} -p {port} -U {user_name} -d {db_name}"
    # -s: raster SRID
    # -a: append to table (assumes it's been create with 'create()')
    # -C: apply all raster Constraints
    # -P: pad tiles to guarantee all tiles have the same width and height
    # -M: vacuum analyze after import
    # -t: specifies the pixel size of each row. Important to keep low for performance!
    
    cmd_raster2pgsql = cmd_raster2pgsql.format(**{'filename': path.join(TILES_DIR, '*.tif'),
                                                  **pg_settings})
    
    proc = subprocess.Popen(cmd_raster2pgsql,
                         stdout=subprocess.PIPE, 
                         stderr=subprocess.STDOUT,
                         shell=True,
                         env=env_current
                         )
    
#    for line in proc.stdout:
#        log.debug(line.decode())
#    proc.stdout.close()
    return_code = proc.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd_raster2pgsql)