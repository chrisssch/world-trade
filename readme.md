# World Trade 

Author: Christoph Schauer </br>
Uplodaded: ??? </br>
Last update: 2020/01/10 </br>


## Introduction 

This repository contains a couple of notebooks and for showcasing Apache Spark - ETL operations, queries, and machine learning - with a big-enough-but-not-too-big dataset.  This repository consists of 3 parts:
* Compilation of the dataset: Conversion of raw data to parquet
* SQL-like queries on the dataset both in PySpark proper as well as PySpark's SQL API
* Estimation of a [gravity model of international trade](https://en.wikipedia.org/wiki/Gravity_model_of_trade)


## Description of the data 

* <b> Trade data</b>: Monthly data on bilateral trade flows between each EU country and all countries on the globe on the 8-digit level of the Combined Nomenclature (about 30,000 product categories) for the last 20 years, plus meta data. The raw data files were downloaded from [Eurostat's COMEXT database](https://ec.europa.eu/eurostat/web/international-trade-in-goods/data/focus-on-comext) and is about 60 GB in size. The raw data was compiled into parquet files, one for each year, with the script in `convert-data-to-parquet.ipynb`.
* <b>Distance and geographical data</b>:Data with metrics capturing the distance between two trade partners, such as the distance between thier capitals, common borders, common languages, etc). This data was downloaded from [Centre d'Études Prospectives et d'Informations Internationales (CEPII)](http://www.cepii.fr/cepii/en/bdd_modele/bdd.asp). This data is required for estimating the gravity equation 
* <b>GDP Data</b>: Downloaded from the [World Bank](https://databank.worldbank.org/reports.aspx?source=2&series=NY.GDP.MKTP.KD&country=#).


## Files 

* `convert-dat-to-parquet.ipynb`: Contains a script for converting the downloaded raw .dat files to .parquet files
* `data-queries-spark.ipynb`: Sample queries on this dataset in PySpark proper
* `data-queries-sql.ipynb`: Sample queries on this dataset using PySpark's SQL API 
* `gravity-model.ipynb`: Data preparation, definition of pipelines, and estimation of a gravity model using the compiled dataset in PySpark.
* `spark_lr_summary.py`: Contains a function for printing out summary statistics for a linear regression model along the lines of what's available in other statistical software packages.

## Next Steps
* Download the remaining months in 2019 when they're available
* Add more explanations everywhere
* Add queries the product category descriptions - a bit of NLP basically
* Add missing SQL queries
* Define a custom pipeline component for applying log transforms to columns
* Expand exploratory analysis 
* Do a "proper" residuals analysis