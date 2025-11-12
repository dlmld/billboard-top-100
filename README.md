## billboard-top-100

# A look into the history of the music featured in the Billboard Top 100 using a multi-hop ETL.

A practice project fully programmed by Darren Miller to aid in my development of skills related to Python, SQL, Databricks, AI, APIs, AWS, and Tableau.

------------

This project uses the following Kaggle datasest: https://www.kaggle.com/datasets/ludmin/billboard/data

The dataset must be imported to AWS as an S3 using a Lambda function, then it can be pulled into Databricks. The dataset is updated weekly every Wednesday at 02:00 (server time). The AWS S3 can be set on a scheduler using EventBridge to update weekly some time after the dataset weekly updates.

See the Lambda function (lambda_function.py) for the dataset import.

The ETL file is called:

billboard-top-100 Notebook 

This ETL includes data enrichment via Spotify's free Search API and Reccobeat's free Audio Features API. Includes Gold layer aggregation into several tables for insights about tracks, artists, annual trends, decade trends, and more. Includes an AI layer in the loop that intelligently adjusts the 'Song' and 'Artist' query such that the track can be found in cases where the original Kaggle dataset wording or formatting of the song or artist are atypical or include symbols that Spotify's Search API cannot handle well.

Note that the code currently filters down to just the Top 5, rather than processing the full Top 100. This is to limit the cost, processing time, and volume of data since this project is just for practice and demonstrating my programming and data engineering skills development.
