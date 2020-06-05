import gzip, shutil, os
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from geopandas_postgis import PostGIS


def _convert_file_format(input_dir, output_dir, file, output_format):
    """
    Convert the file format
    """
    file_name = file.split('.')[:-1][0]

    # read in data file    
    gdf = gpd.read_file('%s' % (os.path.join(input_dir, file)))

    # write file in new format
    drivers = {'shp':'', 'geopackage':'GPKG', 'gpkg':'GPKG', 'geojson':'GeoJSON'}

    # set the driver to use if required
    if output_format not in drivers.keys():
        driver_type = output_format
    else:
        driver_type = drivers[output_format]

    if output_format == 'shp':
        gdf.to_file('%s.%s' % (os.path.join(output_dir, file_name), output_format))
    else:
        gdf.to_file('%s.%s' % (os.path.join(output_dir, file_name), output_format), driver=driver_type)
    

def convert_files(input_dir, output_dir, input_format, output_format):
    """
    Convert a set of files to a different format.
    """

    files = []
    for f in os.listdir(data_dir):
        files.append(f)

    for file in files:
        if file.split('.')[-1] == '%s' % input_format:
                         
            _convert_file_format(input_dir=input_dir, output_dir=output_dir, file=file,  output_format=output_format)


def extract_archive(input_list, output_dir, archive_format='gz', input_format='gml', convert_to=None, temp_dir='temp'):
    """
    Read in an archive and extract. Option to convert to a different file format.
    """

    if convert_to is not None:
        temp_path = os.path.join(output_dir,temp_dir)

    for data_dir in input_list:

        files = []
        for f in os.listdir(data_dir):
            files.append(f)

        for file in files:

            if file.split('.')[-1] == '%s' % archive_format:
                file_name = file.split('.')[0]

                if convert_to is not None:
                    save_to=temp_path
                    if not os.path.exists(temp_path):
                        os.mkdir(os.path.join(output_dir,temp_dir))
                else:
                    save_to=output_dir

                with gzip.open('%s' % (os.path.join(data_dir, file)), 'rb') as f_in:
                                            
                    with open('%s.%s' % (os.path.join(save_to, file_name), input_format), 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                if convert_to is not None:
                    _convert_file_format(input_dir=temp_path, output_dir=output_dir, file=file.replace('.%s' % archive_format,''), output_format=convert_to)

    # at end, delete temp directory
    if convert_to is not None:
        shutil.rmtree(temp_path)


def write_to_database(merge, data_dir, database, schema_name, table_name, username, password, host, port, format='gml'):
    """
    """
    engine = create_engine("postgresql+psycopg2://%s:%s@%s:%s/%s" % (username, password, host, port, database))

    files = []
    for f in os.listdir(data_dir):
        files.append(f)

    first = True
    for file in files:

        file_name = file.split('.')[0]

        if file.split('.')[-1] == format:

            if merge is True and first is False:
                gdf_ = gpd.read_file('%s' % (os.path.join(data_dir, file)))
                gdf = gdf.append(gdf_, ignore_index=True)
            else:
                first = False
                gdf = gpd.read_file('%s' % (os.path.join(data_dir, file)))

            if gdf.crs == '':
                gdf.crs = "EPSG:27700"

            if gdf.geom_type[0] is None:
                geom_type = 'LineStringZ'
            else:
                geom_type = str(gdf.geom_type[0])
                if geom_type == 'LineString':
                    geom_type = 'LineStringZ'
            
            if merge is False:
                gdf.postgis.to_postgis(con=engine, table_name=file_name.lower(), schema=schema_name, geometry=geom_type)

        print('Written %s to database' % file_name )

    if merge is True:
        print('Writing data to database')
        gdf.postgis.to_postgis(con=engine, table_name=table_name, schema=schema_name, geometry=geom_type)

