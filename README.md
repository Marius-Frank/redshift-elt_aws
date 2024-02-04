# Sparkify ELT Project

## Table of Contents

[Introduction](#introduction)
[Database Schema Design](#database-schema-design)
[ETL Pipeline](#database-schema-design)
[Usage and Repository Structure](#usage-and-repository-structure)

## Introduction

The Sparkify ELT (Extract, Load, Transform) project is a data engineering project that aims to help Sparkify, a music streaming startup, analyze and gain insights from their user activity data and song metadata. As Sparkify's user base and song database have grown, they need a scalable solution to process and analyze this data efficiently. The project focuses on moving their data onto the cloud, specifically using Amazon Redshift, to provide an easily accessible and performant analytics platform.

## Database Schema Design

The purpose of this database is to provide Sparkify with a robust and scalable platform for analyzing their user activity and song data. By centralizing and transforming their data into a star schema, Sparkify's analytics team can run complex queries to gain insights into user behavior, song popularity, and other relevant metrics. The database will help Sparkify answer questions such as:

What songs are users listening to the most?
How does user engagement change over time?
What are the most active user demographics?
Database Schema Design

To achieve the analytical goals of Sparkify, we have designed a star schema for the database. The star schema is optimized for querying and allows Sparkify's analytics team to easily aggregate data and gain insights.

The schema consists of the following tables:

**Staging Tables**
- staging.events: Contains data from JSON logs on user activity.
- staging.songs: Contains data from JSON metadata on songs.
  
**Dimension Tables**
- analytics.users: Contains user information.
- analytics.songs: Contains song information.
- analytics.artists: Contains artist information.
- analytics.time: Contains time-related information.
  
**Fact Table**
- analytics.songplays: Contains records of song plays and related information.

## ETL Pipeline

The ETL (Extract, Load, Transform) pipeline for this project involves the following steps:

1. Data is extracted from Sparkify's data sources, which are JSON logs and metadata stored in Amazon S3.
2. The data is loaded into staging tables (staging.events and staging.songs) in Amazon Redshift.
3. Data is transformed and loaded into the dimension and fact tables (analytics.users, analytics.songs, analytics.artists, analytics.time, and analytics.songplays) by executing SQL queries defined in sql_queries.py.
The ETL pipeline is designed to be automated and can be scheduled to run at regular intervals to keep the database up-to-date with new data.

## Usage and Repository Structure

To use this project, follow these steps:

1. Configure your AWS credentials and database connection details in the dwh.cfg file.
2. Run the create_tables.py script to create the necessary tables in your Redshift cluster.
3. Run the etl.py script to execute the ETL pipeline, which extracts data from S3, loads it into staging tables, and transforms it into the star schema.
4. Ensure that you have the required Python libraries (psycopg2) installed and access to an AWS Redshift cluster.