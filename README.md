# LocustCovid19

Python library for locustcovid19 project

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install required packages

```bash
pip3 install -r requirements.txt
```

Unzip the repository compressed directory:

```bash
unzip -d locustcovid19 locustcovid19.zip
```

## Usage

cd one level up from your locustcovid19 directory and run:

```bash
python3 locustcovid19 
```

## Configuration

locustcovid19 uses an application.yaml file for configuration:

#### config/application.yaml

```yaml
environment: 'test'
data:
        landing: 's3://mercy-locust-covid19-landing-test/'
        reporting: 's3://mercy-locust-covid19-reporting-test/'
module: 'croplands'
```

environment is selfexplanatory, can be production or test

data holds the location of the input bucket (landing) and output bucket (reporting) on s3

module is the python module that will be executed, valid module names are location, shapefile, production, population, measure, demand, cropland and forageland

## Modules

1. Location 

   location_table.py  

   output: location_dim/location_table.csv


2. Measures 

   measure_table.py  

   output:  Measures_csv.csv 
     
3. Price 

   price_table.py 

   input: wfpvam_foodprices.csv in landing and /Date_Dim/Date_Dim.csv

4. Production

   production_table.py  
   
   input: FAOSTAT_data.csv in landing and location_dim 

5. Demand 

   demand_table.py 
   
   input: population files 
   
6. Population: population once a year just one year 

   population_table.py  
   
7. Croplands
  
   cropland_area.py

   input: crops_*csv files and GFSAD_tif
 
   cropland_locust.py  

   input: cropland/crops_locust_distr_*.csv and mercy-locust-covid19-landing-test/Swarm_Master.shp 

8. Forage:

   forageland_area.py  

   input: /Date_Dim/Date_Dim.csv, forageland and Swarm_Master.shp
   
   forageland_locust.py 
   
   input: Swarm_Master.sh, Swarm_Master.shx

There are several files in the root folder. The code goes in *locustcovid19".

- *requirements.txt* has all the non-standard packages that are needed to run our code (pandas, numpy)
- *MANIFEST.in* has a list of the folders that will be included when the package is generated. It has to be edited.
- *.gitignore* contains a list of file specifications that will not be part of the repository
- *setup.py* has the installation instructions for our package. It follows the setuptools guidelines. Running "python setup.py install" will install the package in our current environment.

## Folder structure
- *locustcovid19* is the folder that has the module code. 
- *config* yaml configuration files
- *tests* is the folder where python unittests are run
- *utils* have auxiliary script that may be needed to make the package work, for example table creation scripts
- *extras* this is a cointainer for anything that might be useful but is not part of the package itself. For example Jupyter notebooks, log outputs, test files, etc.
