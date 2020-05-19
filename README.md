# Data Modeling - PostgreSQL
-------
*Udacity Project for Data Engineer Nanodegree*
## Summary of the Project
In this project, you’ll model user activity data for a music streaming app called Sparkify. The project is done in two parts. You’ll create a database and import data stored in CSV and JSON files, and model the data. You’ll do this first with a relational model in Postgres, then with a NoSQL data model with Apache Cassandra. You’ll design the data models to optimize queries for understanding what songs users are listening to. For PostgreSQL, you will also define Fact and Dimension tables and insert data into your new tables. For Apache Cassandra, you will model your data to help the data team at Sparkify answer queries about app usage. You will set up your Apache Cassandra database tables in ways to optimize writes of transactional data on user sessions.

* with Postgres
    * Students will model user activity data to create a database and ETL pipeline   
    in Postgres for a music streaming app. They will define Fact and Dimension tables   
    and insert data into new tables.  
    
   
* with Apache Cassandra
    * Students will model event data to create a non-relational database and ETL pipeline   
    for a music streaming app. They will define queries and tables for a database built using   
    Apache Cassandra.


## Repository structure
* analytics - shows data visualization and analysis of the results
* data folder - contains json files with datasets
* create_tables - creates the database and tables accordint to sql_queries
* etl_fast - additional content, I created a faster alternative to etl.py
* sql_queries - lists all neccessary queries for CREATE, UPSERT, DROP
* test: test environment for testing queries


## Project Objective
### Purpose
* Transform raw event data from flat files into analytics-ready database
    * Create the tables in PostgreSQL (create_tables.py)
    * Load the data from the song attributes and fill the song and artist table
    * Load the data from the logs and fill the time, user, songplay (fact) table
    
    
* Data modeling: Star Schema with Facts (Transactional) and Dimensions (Master Data)
    * User axis
    * Song axis
    * Artist axis
    * Time axis
   
   
* Note: The schema is not 3NF (the fact table duplicates information from the song and artist tables)


### How to run the standard Python scripts
* Open Terminal
    * python create_tables.py
    * python etl.py

   
## Additional Content
### Faster etl
* The official documentation for PostgreSQL features an entire section on Populating a Database. \
    According to the documentation, the best way to load data into a database is using the copy command.
* The COPY command is much faster than the INSERT
* Therefore I created a separate etl to do exactly that,
    * I iterate the json files into a csv like data structure 
    * which i then copy into a temp table 
    * from which i upsert into the destination table
    * json --> csv-string --> copy --> temp table --> destination table

### Analytics
* I analyzed and visualized the datasets to answer the following questions:
    * Which songs are users listening to the most (Top15)?
    * When are most users active during the day?
    * When are most users active during the month?
    * Where are most users from (Top20)?
    * How many paying users?

### How to run the faster Python scripts + analytics
* Open Terminal
    * python create_tables.py
    * python etl_fast.py
    
    
* Open analytics.ipynb in jupyter notebook and run from top to bottom   
OR just open ipynb in the git repository to see the results