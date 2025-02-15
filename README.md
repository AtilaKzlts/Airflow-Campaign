![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Bar.svg)

# Campaign Performance Tracking

## Project Introduction
In this project, a data pipeline was created for the marketing department to track the performance of three different campaigns and was supported by a live dashboard updated monthly. Since the purpose of the campaigns is promotion, click and CPC (Cost per click) metrics are kept in the foreground as the most important metrics.

## Steps

![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/airflowsheets.svg)

+ **Data Collection**: Required data was pulled from AWS S3.

+ **Data Cleaning and Processing**: Data cleaning operations including calculation of metrics like CTR and CPC were performed using Apache Airflow script.

+ **Data Storage**: Cleaned data was uploaded to Google Sheets via API for sharing and small size.

+ **Visualization**: Processed data was used to create reports and live dashboards using Tableau.

+ **Automation**: Apache Airflow DAG is run monthly to ensure continuous data update.


## Snapshot

![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Dashboard%201.png)
