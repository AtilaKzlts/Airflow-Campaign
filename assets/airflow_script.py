import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import logging
from io import StringIO
import gspread
from google.oauth2.service_account import Credentials
from airflow.utils.email import send_email
import os
import json



def send_success_email(context):
    """Send an email when all tasks are successful."""
    ti = context['ti']

    # Fetch transformed data from XCom
    df_json = ti.xcom_pull(task_ids='transform_task', key='transformed_data')

    if df_json is None:
        raise ValueError("Transformed data is empty, cannot generate email!")

    # Convert JSON data to DataFrame
    df = pd.read_json(df_json)

    # Calculate metrics
    total_clicks = df['clicks'].sum()
    total_impressions = df['impressions'].sum()
    total_cost = df['cost'].sum()
    average_ctr = (df['clicks'] / df['impressions']).mean()
    average_cpc = (df['cost'] / df['clicks']).mean()

    email_subject = "📊 Campaign Tracking System"

    # Create the email content with proper HTML formatting
    email_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, Helvetica, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 600;
                margin: 0 auto;
                padding: 20px;
            }}
            .header {{
                background-color: #4285f4;
                color: white;
                padding: 15px;
                text-align: center;
                border-radius: 5px 5px 0 0;
            }}
            .content {{
                background-color: #f9f9f9;
                padding: 20px;
                border-radius: 0 0 5px 5px;
                border: 1px solid #ddd;
            }}
            .metric {{
                margin: 10px 0;
                padding: 10px;
                background-color: white;
                border-left: 4px solid #4285f4;
            }}
            .metric-label {{
                font-weight: bold;
                display: inline-block;
                width: 65%;
            }}
            .metric-value {{
                font-weight: bold;
                color: #4285f4;
            }}
            .links {{
                margin-top: 20px;
                text-align: center;
            }}
            .btn {{
                display: inline-block;
                padding: 10px 15px;
                background-color: #FFAB5B;
                color: white !important;
                text-decoration: none;
                border-radius: 4px;
                margin: 10px;
            }}
            .footer {{
                margin-top: 20px;
                text-align: center;
                font-size: 12px;
                color: #777;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>📊 Campaign Tracking Automation</h2>
        </div>
        <div class="content">
            <p>Hello 👋,</p>
            
            <p>The campaign tracking automation has been <strong>successfully completed!</strong> Here are the key statistics:</p>
            
            <div class="metric">
                <span class="metric-label">🖱️ Total Clicks:</span>
                <span class="metric-value">{total_clicks:,}</span>
            </div>
            
            <div class="metric">
                <span class="metric-label">👀 Total Impressions:</span>
                <span class="metric-value">{total_impressions:,}</span>
            </div>
            
            <div class="metric">
                <span class="metric-label">💸 Total Cost:</span>
                <span class="metric-value">£{total_cost:,.2f}</span>
            </div>
            
            <div class="metric">
                <span class="metric-label">📈 Average CTR (Click-Through Rate):</span>
                <span class="metric-value">{average_ctr:.2%}</span>
            </div>
            
            <div class="metric">
                <span class="metric-label">💰 Average CPC (Cost Per Click):</span>
                <span class="metric-value">£{average_cpc:.2f}</span>
            </div>
            
            <div class="links">
                <p>The data has been successfully uploaded to <strong>Google Sheets</strong>.</p>
                <a href="https://docs.google.com/spreadsheets/d/109AumgUBXTpF3lRSZ3haRqkokxyEJ-dl5JrZGg5BOfw/edit?gid=0#gid=0" class="btn">View Google Sheets</a>
                
                <p>Check out the <strong>Tableau Dashboard</strong>:</p>
                <a href="https://public.tableau.com/app/profile/atilla.kiziltas/viz/airlfow_2/Dashboard1" class="btn">View Dashboard</a>
            </div>
        </div>
        <div class="footer">
            <p>Thank you and best regards!<br>
            Campaign Tracking System</p>
        </div>
    </body>
    </html>
    """


    logging.info("Sending email...")  # Debugging email sending

    try:
        send_email(
            to="atilla.kiziltass@gmail.com",
            subject=email_subject,
            html_content=email_content
        )
        logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


# Data pulling function
def pull(**kwargs):
    try:
        logging.info("Pull operation started...")
        
        # Using S3Hook to fetch the file from S3
        s3_hook = S3Hook(aws_conn_id='s3_aws')
        
        # Fetch the file from S3
        data = s3_hook.read_key(key='campaigns_redstone2025.csv', bucket_name='ilk-kovam')

        if not data:
            logging.info("No data fetched from S3...")
            raise ValueError("Data fetched from S3 is empty!")

        # Convert CSV data to a StringIO object
        data_io = StringIO(data)
        
        # Read the data with Pandas
        df = pd.read_csv(data_io, delimiter=',', header=0)
        
        logging.info("Pull operation completed...")

        # Push the data to XCom in JSON format
        kwargs['ti'].xcom_push(key='campaign_data', value=df.to_json(orient='records'))

        logging.info("Data sent to XCom...")

    except Exception as e:
        logging.error(f"Error occurred during data fetch: {e}")
        raise

# Transform data
def transform(**kwargs):
    logging.info("Transform operation started...")
    
    ti = kwargs['ti']

    # Fetch the data from XCom in JSON format
    df_json = ti.xcom_pull(task_ids='extract_task', key='campaign_data')
    
    if df_json is None:
        raise ValueError("Data fetched from XCom is empty!")

    df = pd.read_json(df_json)

    # Check for missing values
    missing_values = df.isnull().sum()
    if missing_values.any():
        logging.warning(f"Missing values found: {missing_values[missing_values > 0]}")

    # Logical checks
    assert (df['clicks'] >= 0).all(), "Clicks value cannot be negative."
    assert (df['impressions'] >= 0).all(), "Impressions value cannot be negative."
    assert (df['cost'] >= 0).all(), "Cost value cannot be negative."

    logging.info(f"{len(df)} rows of data fetched. Applying transform operations...")
    
    # Transform operations
    df['date'] = pd.to_datetime(df['date'])
    df['ctr'] = df['clicks'] / df['impressions']
    df['cpc'] = df['cost'] / df['clicks']

    df = df.drop(columns='Unnamed: 0')
    
    logging.info("Transform operation completed. Sending data to XCom...")
    
    # Push the transformed data to XCom in JSON format
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json())
    logging.info("Transform operation completed. Data sent to XCom.")


# Convert dates to string format
def convert_dates_to_str(df):
    """Convert date columns to string format."""
    for col in df.select_dtypes(include=['datetime']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%d')  
    return df


# Load data to Google Sheets
def load(**kwargs):
    """Load the data to Google Sheets"""
    try:
        ti = kwargs['ti']

        # Fetch the transformed data from XCom in JSON format
        df_json = ti.xcom_pull(task_ids='transform_task', key='transformed_data')

        if df_json is None:
            raise ValueError("Transformed data is empty, load operation cancelled!")

        logging.info("Loading data to Google Sheets...")

        # Convert JSON data to DataFrame
        df = pd.read_json(df_json)

        # Convert dates to string format
        df = convert_dates_to_str(df)

        # Create Credentials object
        scopes = ['https://spreadsheets.google.com/feeds',
                  'https://www.googleapis.com/auth/drive']
        
        creds = Credentials.from_service_account_file("/opt/airflow/dags/credent.json", scopes=scopes)
        logging.info("Authentication successful.")
        
        client = gspread.authorize(creds)
        logging.info("Google Sheets API authorization successful.")

        # Create Google Sheets connection
        client = gspread.authorize(creds)

        sheet_id = "109AumgUBXTpF3lRSZ3haRqkokxyEJ-dl5JrZGg5BOfw"  # Target Google Sheet ID
        sheet = client.open_by_key(sheet_id).sheet1

        # Clear previous data and append new data
        sheet.clear()
        sheet.append_rows([df.columns.tolist()] + df.values.tolist())

        logging.info("Data successfully loaded to Google Sheets.")

    except Exception as e:
        logging.error(f"Error occurred during Google Sheets load: {e}")
        raise


# Default arguments
default_args = {
    'owner': 'Atilla',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
    'on_success_callback': send_success_email,  # Başarıyla tamamlandığında e-posta gönder
}

# Define the DAG
with DAG(
    dag_id='campaigns_follower_with_email',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 11),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=pull,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True,
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task