![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Bar.svg)


<div align="center"> <h1>Campaign Performance Tracking</h1> </p> </div>



## Project Introduction
In this project, a dynamic data pipeline supported by a live dashboard was created to enable the marketing department to track the performance of three different campaigns on a monthly basis starting from January 2025. Since the primary goal of the campaigns was promotion, our main metric was ***impressions***, while key performance indicators such as clicks and Cost Per Click (CPC) were also prioritized as key metrics.

## About the Dataset

| **Column Name**            | **Description**                                          |
|----------------------------|----------------------------------------------------------|
| `campaign_id`               | Unique identifier for each marketing campaign            |
| `campaign_name`             | Name or identifier of the campaign                       |
| `date`                      | Date of the recorded data                                |
| `clicks`                    | Number of clicks generated by the campaign               |
| `impressions`               | Number of times the ad was shown to users                |
| `platform`                  | Advertising platform (e.g., Google Ads, Facebook)        |
| `cost`                      | Total cost of the campaign on the given date (£)       |
| `region`                    | Geographical region of the campaign's performance        |


## Steps

![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/airflowsheets.svg)

+ **Data Collection**: Required data was pulled from AWS S3.

+ **Data Cleaning and Processing**: Data cleaning operations including calculation of metrics like CTR and CPC were performed using Apache Airflow script.

+ **Data Storage**: Cleaned data was uploaded to Google Sheets via API for sharing and small size.

+ **Visualization**: Processed data was used to create reports and live dashboards using Tableau.

+ **Automation**: Apache Airflow DAG is run monthly to ensure continuous data update.

[See DAG](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/airflow_script.py)


## Snapshot

![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Dashboard%201.png) 

![image](https://github.com/AtilaKzlts/Airflow-Campaign/blob/main/assets/Dashboard%202.png) 

[See Dashboard](https://public.tableau.com/app/profile/atilla.kiziltas/viz/airlfow/Dashboard1)
