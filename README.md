![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Bar.svg)

# Campaign Performance Tracking

## Project Introduction
In this project, a data pipeline was created for the marketing department to track the performance of three different campaigns, supported by a live dashboard updated monthly. Since the primary goal of the campaigns is promotion, metrics such as clicks and Cost Per Click (CPC) are prioritized as the key indicators.

This setup allows the marketing team to better analyze the effectiveness of the campaigns, optimize budget usage, and make data-driven decisions to shape future advertising strategies.


## Steps


![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/airflowsheets.svg)

+ **Data Collection**: Required data was pulled from AWS S3.

+ **Data Cleaning and Processing**: Data cleaning operations including calculation of metrics like CTR and CPC were performed using Apache Airflow script.

+ **Data Storage**: Cleaned data was uploaded to Google Sheets via API for sharing and small size.

+ **Visualization**: Processed data was used to create reports and live dashboards using Tableau.

+ **Automation**: Apache Airflow DAG is run monthly to ensure continuous data update.

[See Full Script](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/airflow_script.py)


## Snapshot

![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Dashboard%201.png)

[See Dashboard](https://public.tableau.com/app/profile/atilla.kiziltas/viz/airlfow/Dashboard1)
