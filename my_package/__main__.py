# This file is executed when the module is run from the command line:
# python -m my_package
from my_package.example import greet
from my_package.location_table import LocationTable


if __name__ == '__main__':
    # A dummy main
    print('Running module')

    greet('developer')

    print("It worked!")

    # 1. Creation of location table
    loc_table = LocationTable()
    location_table = loc_table.concat_sub_tables() # Create geodataframe
    loc_table.export_to_parquet(location_table, 'output/location_table') # Export table to parquet format

    # 2. Creation of shapefile table