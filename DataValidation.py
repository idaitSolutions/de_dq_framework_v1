from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, mean, stddev, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType, DoubleType, TimestampType

class DataValidation:
    def __init__(self, df):
        """
        Initialize the DataValidation class with a DataFrame.

        Parameters:
        df (DataFrame): The DataFrame to be validated.
        """
        self.df = df

    def validate_schema(self, expected_schema):
        """
        Validate the schema of the DataFrame.
        """
        actual_schema = self.df.schema

        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        actual_fields = {field.name: field.dataType for field in actual_schema.fields}

        if expected_fields != actual_fields:
            raise ValueError(f"Schema mismatch: Expected {expected_schema} but got {actual_schema}")
        print("Schema validation passed.")

    def check_completeness(self, columns):
        """
        Check for null values in specified columns.
        """
        null_counts = {}
        for c in columns:
            if isinstance(self.df.schema[c].dataType, (IntegerType, DecimalType, FloatType, DoubleType)):
                null_counts[c] = self.df.filter(col(c).isNull() | isnan(col(c))).count()
            else:
                null_counts[c] = self.df.filter(col(c).isNull()).count()

        for col_name, count in null_counts.items():
            print(f"Column {col_name} contains {count} null values")  # Log the null counts
            if count > 0:
                raise ValueError(f"Column {col_name} contains {count} null values")
        print("Completeness check passed.")

    def check_data_consistency(self, column1, column2):
        """
        Check for data consistency between two columns.
        """
        inconsistent_rows = self.df.filter(col(column1) != col(column2))
        inconsistent_count = inconsistent_rows.count()
        if inconsistent_count > 0:
            print(f"Inconsistent data found between {column1} and {column2}. Sample of inconsistent rows:")
            inconsistent_rows.show()
            raise ValueError(f"Inconsistent data found between {column1} and {column2}")
        print("Data consistency check passed.")

    def check_value_ranges(self, column, min_value, max_value):
        """
        Check if values in a column fall within a specified range.
        """
        out_of_range_count = self.df.filter((col(column) < min_value) | (col(column) > max_value)).count()
        if out_of_range_count > 0:
            raise ValueError(f"Column {column} has {out_of_range_count} values out of range [{min_value}, {max_value}]")
        print("Range and threshold check passed.")

    def check_uniqueness(self, column):
        """
        Check for uniqueness in a specified column.
        """
        unique_count = self.df.select(column).distinct().count()
        total_count = self.df.count()
        if unique_count != total_count:
            raise ValueError(f"Column {column} has duplicate values")
        print("Uniqueness check passed.")

    def verify_transformation(self, column, expected_value):
        """
        Verify that a transformation has been applied correctly.
        """
        mismatched_count = self.df.filter(col(column) != expected_value).count()
        if mismatched_count > 0:
            raise ValueError(f"Transformation error: Column {column} has {mismatched_count} mismatched values")
        print("Transformation verification passed.")

    def statistical_analysis(self, column):
        """
        Perform statistical analysis on a specified column.
        
        Parameters:
        column (str): The column to perform statistical analysis on.
        """
        stats = self.df.select(
            mean(col(column)).alias('mean'),
            expr('percentile_approx({}, 0.5)'.format(column)).alias('median'),
            stddev(col(column)).alias('stddev')
        ).collect()[0]
        
        print(f"Statistical analysis for column {column}:")
        print(f"Mean: {stats['mean']}")
        print(f"Median: {stats['median']}")
        print(f"Standard Deviation: {stats['stddev']}")

# Initialize Spark session

spark = SparkSession.builder \
.appName("Data Validation") \
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

# Load data from MySQL table
df = spark.read.jdbc(url=url, table=table, properties=properties)

# Example expected schema
expected_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("join_date",  TimestampType(), True),
    StructField("salary", DecimalType(18,0), True),
    StructField("category", StringType(), True)
])


# Instantiate DataValidation class
data_validator = DataValidation(df)

# Perform validations
data_validator.validate_schema(expected_schema)
data_validator.check_data_consistency("name", "salary")
data_validator.check_value_ranges("age", 18, 65)
data_validator.check_value_ranges("salary", 30000.0, 200000.0)
data_validator.check_uniqueness("id")
data_validator.statistical_analysis("age")
data_validator.statistical_analysis("salary")
data_validator.check_completeness(["id", "name", "age", "join_date", "salary", "category"])

# Example transformation verification
df_transformed = df.withColumn("salary_double", col("salary") * 2) 
data_validator_transformed = DataValidation(df_transformed)
data_validator_transformed.verify_transformation("salary_double", col("salary") * 2)

# Stop Spark session
spark.stop()
