# ETL with PostgreSQL
-------


## Project Objective
### Model user activity for a music streaming app


* Transform raw event data from flat files into analytics-ready database
    * create a database in PostgreSQL
    * Load the data from the song attributes and fill the song and artist table
    * Load the data from the logs and fill the time, user, songplay (fact) table


* Data modeling: Star Schema with Facts (Transactional) and Dimensions (Master Data)
    * User axis
    * Song axis
    * Artist axis
    * Time axis

## Repository structure
* analytics - shows data visualization and analysis of the results
* data folder - contains json files with datasets
* create_tables - creates the database and tables according to sql_queries
* etl_faster - a faster alternative to etl.py
* sql_queries - lists all neccessary queries for CREATE, UPSERT, DROP

* Note: The schema is not 3NF (the fact table duplicates information from the song and artist tables)


### How to run the standard Python scripts
* Open Terminal
    * python create_tables.py
    * python etl.py


## A faster Alternative

* The COPY command is MUCH faster than the INSERT command
* Therefore I created a separate etl to do exactly that,
    * I iterate the json files into a csv like data structure
    * which i then copy into a temp table
    * from which i upsert into the destination table
    * json --> csv-string --> copy --> temp table --> destination table

## Analytics
* I analyzed and visualized the datasets to answer the following questions:
    * Which songs are users listening to the most (Top15)?
    * When are most users active during the day?
    * When are most users active during the month?
    * Which cities have the highest user activity (Top20)?
    * How many paying users?

### How to run the faster Python scripts + analytics
* Open Terminal
    * python create_tables.py
    * python etl_faster.py


* Open analytics.ipynb in jupyter notebook and run from top to bottom   
OR just open ipynb in the git repository to see the results
