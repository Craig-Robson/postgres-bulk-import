import functions


def to_shp():
    """
    Convert data within a .gz archive into gml and shapefiles.

    """
    data_dir_input = ['']
    data_dir_output = ''
    functions.gml_to_shp(data_dir_input, data_dir_output)


def to_database():
    """
    Convert data via geopandas and write to a postgres-postgis database, either as a single table or as a table per file. Input formats should be compatable with geopandas.

    """
    database = ''
    schema_name = ''
    table_name = ''
    username = ''
    password = ''
    host = ''
    port = ''
    format = ''
    data_dir = ''
    merge = True

    functions.write_to_database(merge, data_dir, database, schema_name, table_name, , username, password, host, port, format)
