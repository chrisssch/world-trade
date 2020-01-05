# Readme 

## World Trade *-WIP-*

This repository contains a couple of notebooks for practicing and showcasing Apache Spark (PySpark) for with a big-enough-but-not-too-big dataset.  As of now this repository consists of 3 parts:
* Compilation of the database
* SQL-like queries on the data in PySpark proper as well as PySpark's SQL "interface"
* Estimation of a Gravity Model

Not sure though where to take this from there.


### Data 

#### Trade data
The data contains monthly data on bilateral trade flows between all EU countries and all other countries on the globe for the last 16 years. This dataset is compiled from the [COMEXT trade database from Eurostat](https://ec.europa.eu/eurostat/web/international-trade-in-goods/data/focus-on-comext). This dataset is about 50 GB in size. The data was downloaded as individual .dat files for each month
from [here](https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=comext) and then converted to .parquet files.

#### Distance data
Data on the distance between two trade partners (like distance between the capitals, common borders, common languages, sea route availability, etc) was downloaded from [Centre d'Études Prospectives et d'Informations Internationales (CEPII)](http://www.cepii.fr/cepii/en/bdd_modele/bdd.asp). This data is required for estimating the gravity equation 

### Files 

* `convert-dat-to-parquet.ipynb`: Contains a script for converting the downloaded .dat files to .parquet files
* `data-queries-spark.ipynb`: Contains sample queries on this dataset in PySpark
* `data-queries-sql.ipynb`: Contains sample queries on this dataset using PySpark's SQL "interface". 
* `gravity.ipynb`: *-WIP-* I'm estimating a version of the [Gravity Model of Trade](https://en.wikipedia.org/wiki/Gravity_model_of_trade) using the COMEXT trade data, CEPII distance data, and GDP data from the World Bank.


### TO DO

* Write a nicer readme
* Add more comments to code
* Add short descriptions for all steps
* Add more SQL queries
* Estimate Gravity Model properly
* Download 4 more years plus the remaining months in 2019 when they're available
* etc...