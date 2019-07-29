# Cloud Data Warehouse
**Version 1.0.0**

## Table of contents
* [Intro](#Intro)
* [Motivation](#Motivation)
* [Files](#Files)
* [Technologies](#Technologies)
* [Libraries](#Libraries)
* [Install](#Install)
* [Analysis](#Analysis)
* [Acknowledgement](#Acknowledgement)

## Intro
The purpose of the Sparkify database is to gather data from files in cloud storage, and generate populated tables hosted on Amazon's data warehouse -Redshift.  The tables are organized into star schema which consists of a fact table and dimension tables allowing for an organized and intutive relationship between columnar tables. This schema provides end-users with a simplified method to making connections between tables, resulting in easier to query data organization.  

## Motivation
The components of this project encompass important aspects of data warehousing. The design of this project begins with the implementation of a star schema using fact and dimension related tables. In order to populate the tables, data is extracted from two different sources - song and logs files which are hosted in an amazon s3 bucket. The data is loaded into staging tables in Redshift, which then becomes the soure of data for the fact and dimensions table.  The dimension tables are assigned a distribution all style based on size, so that the table is distributed across all the slices of data in the nodes for a given cluster.  As a result, there is a greater efficiency in joining tables so those results can obtained at a faster rate.  In order increase data querying efficency, all tables are provided with sort keys which orders the tables on the basis of the slected column - when appropriately selected, iterations through a table to locate a value are reduced.  For these tables - sortkeys were selected on the basis of which column is mostly likely to be used as a part of a query, and which column is most likely to be the joining column in a query.  Within the fact table, a distibution key is assigned which  determines row distribition and places mathcing values on the same node.  As a whole- this project builds on the previous relational database project, and incorporates new and relevant concepts such as columnar tables cloud computing/hosting and designating keys for query efficiency.   

Example fact and dimension tables:
![alt text](http://www.zentut.com/wp-content/uploads/2012/10/fact-table-example1.png)
Image provide by ZenTut website. 


Example of Redsift Table Distribution 
![alt text](https://res.cloudinary.com/hrscywv4p/image/upload/c_limit,fl_lossy,h_9000,w_1200,f_auto,q_auto/v1/1483830/Redshift_Table_Distribution_xdsh8u.png)
Image provide by Dzone websie


## Files
- dwh.cfg: This file contains all necessary AWS credentials and data. Contents from this file are used through-out the other files to extract required credentials and information where necessary
- sql.py: this file contains queries that drop existing tables, creates tables, and copies and inserts data into table.
- create_tables.py: this file connects to a postgres instance and a redshift cluster, drops old tables and then creates ables from sql.py file.
- etl.py: this file extracts data from song and log files in an S3 bucket, loads data to staging tables, and then inserts the data to a fact table and dimension tables.
- test.ipynb: this notebook is used to perform basic data checks on data inserted into the tables.
- basicStats.ipynb: used to draw some basic user and song analysis. 


## Technologies
- AWS
- Redshift
- S3
- Python 3.6
- jupyter
- SQL (Postgres)


## Libraries 
-psycopg2
-configparser

## Install
Use [pip](https://pip.pypa.io/en/stable/)(package manager) to install psycopg2  and configparser.

```bash
pip intall psycopg2
pip install configparser
```


## Analysis 

### Demogoraphics
```
--Total Users
select count(*) from users

--User Data Checks
Select count(*) as NumUsers from users;

--Percent Paid User by Identified Gender
Select count(*) as FemaleUsers from users where gender = 'F';

Select count(*) as MaleUsers from users where gender = 'M';

--Level Data Checks
Select count(*) as PaidUsers from users where level = 'paid';

```
### Artist/Song Analysis
```
--Top 10 Longest Songs
Select *, duration/60 as Minutes from songs 
order by duration desc
limit 10;

--Most Popular Artist
SELECT a.name, s.artist_id, count(*) from songs s, artists a
where s.artist_id = a.artist_id
Group BY s.artist_id, a.name
order by 3 desc
limit 10;

--Most Popular Artist Amongst Paid Customers
select a.artist_id, a.name, count(*)
from songplay sp, artists a, songs s
where sp.artist_id = a.artist_id 
and sp.song_id = s.song_id 
and sp. level = 'paid'
Group By a.artist_id, a.name
order by 3 desc
limit 10;


```

## Acknowledgement
- Facts and Dimensions Table layout above provide by ZenTut.
- Distribution Style Examples above provide by DZone.
- This project was peformed as part of the Udacity Data Engineering Nanodegree Program.