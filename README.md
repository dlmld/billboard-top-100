# billboard-top-100
A look into the history of the music featured in the Billboard Top 10, using Databricks to process the data via a multi-hop architecture ETL.

This uses the following Kaggle datasest: https://www.kaggle.com/datasets/ludmin/billboard/data

The dataset must be imported to AWS as an S3 using a Lambda function, then it can be pulled into Databricks. The dataset is updated weekly every Wednesday at 02:00 (server time). The AWS S3 can be set on a scheduler using EventBridge to update weekly some time after the dataset weekly updates.

See the Lambda function (lambda_function.py) for the dataset import.

The ETL file is called:

billboard-top-100 BRONZE SILVER GOLD - includes data enrichment via Spotify's free Search API and Reccobeat's free Audio Features API. Includes Gold layer aggregation into several tables for insights about tracks, artists, annual trends, decade trends, and more.
