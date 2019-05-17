# Agency Performance Model

## Summary

This API provides services for working with the Kaggle dataset owned by ADM2752836 and downloadable at https://www.kaggle.com/moneystore/agencyperformance.

## Approach

The entire service is written in Python 3.6.

### ETL

The dataset is downloaded via Kaggle's API and stored raw in a SQLite table with Pandas using a SQLAlchemy engine. 
Simple cleansing is performed and a star schema is then composed with Pandas and inserted into the same SQLite database via the same method.

### API

The API is built on the Flask framework. 
Each resource posesses a GET method which will formulate and execute a query against the SQLite database based on supplied URL parameters.

## Deployment Method

All components of this service are hosted on an AWS EC2 Linux AMI t2.micro instance. 

### ETL

The ETL script runs daily on a cron job. 

### API

For deploying the Flask API, I closely followed the wiki at https://github.com/eugeneYWang/RestAPI_flask/wiki/Building-Restful-APIs-on-AWS-for-Everyone.
Hence, nginx is used as web server, GUnicorn as WSGI server, and supervisor for running the application in the background

## Usage

### /details

Returns a JSON array of the raw rows of data for a specified agency and product line.

#### Example

curl 'http://18.228.38.173/details?AGENCY_ID=3&PROD_LINE=CL'

#### Required parameters:

* AGENCY_ID
* PROD_LINE

#### Optional parameters

* PRIMARY_AGENCY_ID
* PROD_ABBR
* STATE_ABBR
* VENDOR
* STAT_PROFILE_DATE_YEAR
* AGENCY_APPOINTMENT_YEAR 
* PL_START_YEAR
* PL_END_YEAR
* COMMISIONS_START_YEAR
* COMMISIONS_END_YEAR
* CL_START_YEAR
* CL_END_YEAR
* ACTIVITY_NOTES_START_YEAR
* ACTIVITY_NOTES_END_YEAR

### /summary

Returns a JSON object summarizing all numeric fields (QTY, AMT, etc.) aggregated according to a supplied SQL operator.

#### Example

curl 'http://18.228.38.173/summary?AGG=SUM&AGENCY_ID=3&PROD_LINE=CL'

#### Required parameters

* AGG - Any valid SQLite aggregation operator. (i.e. SUM, AVG, MIN, MAX, etc.)

#### Optional parameters

* AGENCY_ID
* PRIMARY_AGENCY_ID
* PROD_LINE
* PROD_ABBR
* STATE_ABBR
* VENDOR
* STAT_PROFILE_DATE_YEAR
* AGENCY_APPOINTMENT_YEAR 
* PL_START_YEAR
* PL_END_YEAR
* COMMISIONS_START_YEAR
* COMMISIONS_END_YEAR
* CL_START_YEAR
* CL_END_YEAR
* ACTIVITY_NOTES_START_YEAR
* ACTIVITY_NOTES_END_YEAR

### /report

Downloads a CSV file containing the sum total of premiums for each agency and product line in the specified product date range.

#### Example

curl 'http://18.228.38.173/report?MIN_PL_START_YEAR=2010&MAX_PL_START_YEAR=2011'

#### Optional parameters

* MIN_PL_START_YEAR
* MAX_PL_START_YEAR
* MIN_PL_END_YEAR
* MAX_PL_END_YEAR
