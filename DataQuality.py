import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, mean, stddev, skewness
from decimal import Decimal
from pyspark.sql.types import TimestampType, NumericType, BooleanType

class DataQuality:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_data_from_azure_sql(self, url: str, table: str, properties: dict):
        self.df = self.spark.read.jdbc(url=url, table=table, properties=properties)
        return self.df

    def handle_nulls(self, column_name: str, fill_value=None):
        if fill_value is not None:
            self.df = self.df.na.fill({column_name: fill_value})
        else:
            self.df = self.df.na.drop(subset=[column_name])
        return self.df

    def detect_nulls(self, column_name: str):
        null_count = self.df.filter(col(column_name).isNull()).count()
        return null_count

    def detect_timestamp_issues(self, column_name: str):
        if not isinstance(self.df.schema[column_name].dataType, TimestampType):
            raise ValueError(f"Column {column_name} is not of TimestampType")

        def is_invalid_timestamp(date_str):
            try:
                timestamp = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                if timestamp < datetime.datetime(1900, 1, 1) or timestamp > datetime.datetime.now():
                    return True
                return False
            except (ValueError, TypeError, OverflowError):
                return True

        is_invalid_timestamp_udf = udf(is_invalid_timestamp, BooleanType())
        invalid_timestamps = self.df.filter(is_invalid_timestamp_udf(col(column_name))).count()

        return invalid_timestamps

    def validate_timestamp_format(self, column_name: str, format: str):
        @udf(returnType=BooleanType())
        def is_valid_format(date_str):
            try:
                datetime.datetime.strptime(date_str, format)
                return True
            except (ValueError, TypeError):
                return False
        
        invalid_format_count = self.df.filter(~is_valid_format(col(column_name))).count()
        return invalid_format_count

    def detect_all_timestamp_issues(self):
        timestamp_columns = [field.name for field in self.df.schema.fields if isinstance(field.dataType, TimestampType)]
        issues = {}
        for column in timestamp_columns:
            issues[column] = {
                "invalid_timestamps": self.detect_timestamp_issues(column),
                "invalid_format": self.validate_timestamp_format(column, '%Y-%m-%d %H:%M:%S')
            }
        return issues

    def detect_duplicates(self):
        duplicate_count = self.df.count() - self.df.dropDuplicates().count()
        return duplicate_count
    
    def detect_outliers(self, column_name: str, n_std_dev: int = 3):
        if not isinstance(self.df.schema[column_name].dataType, NumericType):
            raise ValueError(f"Column {column_name} is not of NumericType")
        
        stats = self.df.select(
            mean(col(column_name)).alias('mean'),
            stddev(col(column_name)).alias('stddev')
        ).collect()[0]

        mean_value = float(stats['mean']) if isinstance(stats['mean'], Decimal) else stats['mean']
        stddev_value = float(stats['stddev']) if isinstance(stats['stddev'], Decimal) else stats['stddev']

        outliers = self.df.filter(
            (col(column_name) > mean_value + n_std_dev * stddev_value) | (col(column_name) < mean_value - n_std_dev * stddev_value)
        ).count()
        
        return outliers

    def validate_values(self, column_name: str, valid_values: list):
        invalid_values_count = self.df.filter(~col(column_name).isin(valid_values)).count()
        return invalid_values_count

    def detect_skewness(self, column_name: str):
        if not isinstance(self.df.schema[column_name].dataType, NumericType):
            raise ValueError(f"Column {column_name} is not of NumericType")
        
        skew_value = self.df.select(skewness(col(column_name)).alias('skewness')).collect()[0]['skewness']
        return skew_value

    def detect_quality_issues(self):
        quality_issues = {}
        for column in self.df.columns:
            null_count = self.detect_nulls(column)
            quality_issues[column] = {"null_count": null_count}
            if isinstance(self.df.schema[column].dataType, TimestampType):
                timestamp_issues = self.detect_timestamp_issues(column)
                format_issues = self.validate_timestamp_format(column, '%Y-%m-%d %H:%M:%S')
                quality_issues[column]["timestamp_issues"] = timestamp_issues
                quality_issues[column]["format_issues"] = format_issues
            if isinstance(self.df.schema[column].dataType, NumericType):
                outlier_issues = self.detect_outliers(column)
                skewness_value = self.detect_skewness(column)
                quality_issues[column]["outlier_issues"] = outlier_issues
                quality_issues[column]["skewness"] = skewness_value
        duplicates = self.detect_duplicates()
        quality_issues["duplicates"] = {"duplicate_count": duplicates}
        return quality_issues

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Data Quality Checks") \
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

    dq = DataQuality(spark)
    try:
        df = dq.load_data_from_azure_sql(url, table, properties)
        df.show()
        
        # Handle nulls for a specific column
        df = dq.handle_nulls("salary", fill_value="default_value")
        
        # Detect nulls for a specific column
        null_count = dq.detect_nulls("age")
        print(f"Number of null values in the column: {null_count}")
        
        # Detect issues in all timestamp columns
        timestamp_issues = dq.detect_all_timestamp_issues()
        print(f"Timestamp column issues: {timestamp_issues}")
        
        # Detect quality issues in all columns
        quality_issues = dq.detect_quality_issues()
        print(f"Data quality issues: {quality_issues}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

    spark.stop()
