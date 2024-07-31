import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan
from pyspark.sql.types import IntegerType, FloatType, DateType, DecimalType, DoubleType  

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', filename='etl.log')
logger = logging.getLogger(__name__)

class ErrorHandling:
    def __init__(self, teams_webhook_url):
        """
        Initialize the ErrorHandling class with Teams webhook URL.
        
        Parameters:
        teams_webhook_url (str): Webhook URL for sending notifications to Teams.
        """
        self.teams_webhook_url = teams_webhook_url

    def log_error(self, message):
        """
        Log an error message.
        
        Parameters:
        message (str): The error message to log.
        """
        logger.error(message)

    def log_info(self, message):
        """
        Log an info message.
        
        Parameters:
        message (str): The info message to log.
        """
        logger.info(message)

    def send_teams_notification(self, title, message):
        """
        Send a notification to Microsoft Teams.
        
        Parameters:
        title (str): The title of the Teams message.
        message (str): The body of the Teams message.
        """
        try:
            payload = {
                "title": title,
                "text": message
            }
            headers = {
                "Content-Type": "application/json"
            }
            response = requests.post(self.teams_webhook_url, json=payload, headers=headers)
            if response.status_code == 200:
                self.log_info(f"Sent Teams notification: {title}")
            else:
                self.log_error(f"Failed to send Teams notification: {response.status_code} - {response.text}")
        except Exception as e:
            self.log_error(f"Failed to send Teams notification: {e}")

    def handle_error(self, error_message):
        """
        Handle an error by logging it and sending a notification.
        
        Parameters:
        error_message (str): The error message to handle.
        """
        self.log_error(error_message)
        self.send_teams_notification("ETL Process Error", error_message)


    def check_null_values(self, df, columns: list):
        """
        Check for null values in specified columns and send an error notification if any are found.
        
        Parameters:
        df (DataFrame): The DataFrame to check.
        columns (list): List of columns to check for null values.
        """
        try:
            null_counts = {}
            for col_name in columns:
                # Check if the column is numeric and handle NaNs accordingly
                if isinstance(df.schema[col_name].dataType, (IntegerType, FloatType, DoubleType)):
                    null_counts[col_name] = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
                else:
                    null_counts[col_name] = df.filter(col(col_name).isNull()).count()
                    
            nulls_found = {col_name: count for col_name, count in null_counts.items() if count > 0}
            
            if nulls_found:
                error_message = f"Null values found in columns: {nulls_found}"
                self.handle_error(error_message)
            else:
                self.log_info("No null values found in the specified columns.")
        except Exception as e:
            self.handle_error(f"Error checking null values: {e}")


    def check_inconsistent_data(self, df, column1: str, column2: str):
        """
        Check for inconsistent data between two columns and send an error notification if any are found.
        
        Parameters:
        df (DataFrame): The DataFrame to check.
        column1 (str): The first column to check for inconsistency.
        column2 (str): The second column to check for inconsistency.
        """
        try:
            inconsistent_count = df.filter(col(column1) != col(column2)).count()
            if inconsistent_count > 0:
                error_message = f"Inconsistent data found between columns: {column1} and {column2}"
                self.handle_error(error_message)
            else:
                self.handle_error(f"No inconsistent data found between columns: {column1} and {column2}")
        except Exception as e:
            self.handle_error(f"Error checking inconsistent data: {e}")


# Example webhook URL 
teams_webhook_url = "https://beebiconsulting.webhook.office.com/webhookb2/86c5dfea-4780-4724-bd0b-466d38583ec9@f021946a-4fca-4ea9-b514-937c7d7d5083/IncomingWebhook/9b67ef52df31410a84a01bb4f890af02/22221e4d-e892-472f-825e-611fd7b2c677"

# Instantiate ErrorHandling class
error_handler = ErrorHandling(teams_webhook_url)

# Initialize Spark session
spark = SparkSession.builder \
.appName("Data Operations") \
.config("spark.jars", "C:\\mssql-jdbc-12.6.3.jre11.jar") \
.getOrCreate()

# Database connection properties
url = "jdbc:sqlserver://demo-sql-server-001.database.windows.net:1433;database=test_db;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
table = "dq_chk"
properties = {
        "user": "sqladmin",
        "password": "Pa$$word",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }


#error_handler.handle_error("Hello")

try:
    # Load data from MySQL table
    df = spark.read.jdbc(url=url, table=table, properties=properties)
    error_handler.handle_error("Data loaded from MySQL successfully.")

    # Check for null values in specified columns
    columns_to_check_nulls = ["id", "name", "age", "join_date", "salary", "category"]
    error_handler.check_null_values(df, columns_to_check_nulls)

    # Check for inconsistent data between related columns
    error_handler.check_inconsistent_data(df, "name", "salary")


except Exception as e:
    error_handler.handle_error(f"Error loading data from MySQL: {e}")


# Stop Spark session
spark.stop()
