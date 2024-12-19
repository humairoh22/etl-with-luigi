# Simple ETL Pipeline with Luigi

## Overview
An ETL pipeline project to store data from various sources into database PostgreSQL. This project is using luigi tool as data orchestration.

## Problems
- Team Sales have data that store in docker image PostgreSQL. They want to analysis performance of its sales and do forecasting to get best strategy for their sales performance. However, they have problem with their data that have inconsistent format and missing value.
- Team Products have data product pricing in csv format. The problem in their data are having unknown columns that not relevant with their needs, inconsistent value, and duplicates data.
- Team Data Scientist want to do research to find pattern what makes news trending topic. But, they dont have any data needed so that they need help to scrape data from news portal.

## Solutions
Create ETL pipeline to:
- Extract data from different sources. For team DS will scrape data from a website.
- Do data validation to know deeper what inside of these data, the columns that need to transform, and etc.
- Transforming each data source based on what we found when exploring the data.
- Load all data into one database PosgtreSQL.

## Pipeline Design
![pipeline design](https://github.com/humairoh22/etl-with-luigi/blob/master/pic/pipeline.jpg)
## How To Run The Script?
1. Clone the repository.
   ```
   git clone https://github.com/humairoh22/etl-with-luigi.git
   ```
2. Run docker compose to access database where the data from team Sales is stored with this command.
   ```
   cd dockerfile
   docker compose up -d
   ```
3. Activate virtual environment.
   ```
   source venv/bin/activate
   ```
4. Install requirements for this project
   ```
   pip -r requirements.txt
   ```
5. Run the script.
   ```
   python simple_etl.py
   ```



