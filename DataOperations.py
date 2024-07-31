from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

class DataOperations:
    def __init__(self, spark):
        self.spark = spark

    def load_data_from_mysql(self, table_name, jdbc_url, properties):
        return self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

    def join_data(self, df1, df2, join_column, join_type="inner"):
        """
        Join two DataFrames on a common column.

        Parameters:
        df1 (DataFrame): The first DataFrame.
        df2 (DataFrame): The second DataFrame.
        join_column (str): The column to join on.
        join_type (str): The type of join to perform (default is "inner").

        Returns:
        DataFrame: The joined Spark DataFrame.
        """
        return df1.join(df2, on=join_column, how=join_type)

    def group_by(self, df, group_columns, agg_dict):
        """
        Group a DataFrame by specified columns and apply aggregation functions.

        Parameters:
        df (DataFrame): The DataFrame to group.
        group_columns (list): A list of columns to group by.
        agg_dict (dict): A dictionary where keys are column names and values are aggregation functions (e.g., "sum", "avg").

        Returns:
        DataFrame: The grouped and aggregated Spark DataFrame.
        """
        agg_exprs = [eval(f'expr("{func}({col})").alias("{col}_{func}")') for col, func in agg_dict.items()]
        return df.groupBy(group_columns).agg(*agg_exprs)

    def filter_data(self, df, filter_condition):
        """
        Filter a DataFrame based on a condition.
        """
        return df.filter(filter_condition)

    def select_columns(self, df, columns):
        """
        Select specific columns from a DataFrame.
        """
        return df.select(columns)

    def add_column(self, df, column_name, expression):
        """
        Add a new column to a DataFrame based on an expression.

        Parameters:
        df (DataFrame): The DataFrame to add the column to.
        column_name (str): The name of the new column.
        expression (Column): The expression to calculate the new column values (use pyspark.sql.functions.expr).

        Returns:
        DataFrame: The DataFrame with the new column added.
        """
        return df.withColumn(column_name, expression)

    def handle_missing_values(self, df, strategy="drop", subset=None):
        """
        Handle missing value in DataFrame.

        Parameters:
        df (DataFrame): The DataFrame to handle missing values in.
        strategy (str): The strategy to handle missing values ("drop" to drop rows, "fill" to fill with default values).
        subset (list): A list of columns to consider for handling missing values (default is None).

        Returns:
        DataFrame: The DataFrame with missing values handled.
        """
        if strategy == "drop":
            return df.dropna(subset=subset)
        elif strategy == "fill":
            return df.fillna(subset=subset)
        else:
            raise ValueError("Invalid strategy for handling missing values")

    def remove_duplicates(self, df, subset=None):
        """
        Remove duplicate rows from a DataFrame.

        Parameters:
        df (DataFrame): The DataFrame to remove duplicates from.
        subset (list): A list of columns to consider for identifying duplicates (default is None).

        Returns:
        DataFrame: The DataFrame with duplicates removed.
        """
        return df.dropDuplicates(subset=subset)

    def ensure_data_types(self, df, schema):
        """
        Ensure data types for columns in a DataFrame.

        Parameters:
        df (DataFrame): The DataFrame to cast column data types.
        schema (dict): A dictionary where keys are column names and values are the desired data types.

        Returns:
        DataFrame: The DataFrame with columns cast to specified data types.
        """
        for column, dtype in schema.items():
            df = df.withColumn(column, col(column).cast(dtype))
        return df

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

data_ops = DataOperations(spark)

# Load data from MySQL table
df = data_ops.load_data_from_mysql("dq_chk", url, properties)

# Handle missing values (drop rows where age is NULL)
clean_df = data_ops.handle_missing_values(df, strategy="drop", subset=["age"])

# Remove duplicates based on 'id' column
clean_df = data_ops.remove_duplicates(clean_df, subset=["id"])

print(f"Clean dataframe : {clean_df}")

# Ensure data types
schema = {"age": "int", "salary": "float"}
clean_df = data_ops.ensure_data_types(clean_df, schema)

agg_dict = {"salary": "avg"}
# Group and aggregate data
grouped_df = data_ops.group_by(clean_df, ["category"], agg_dict)
print(f"Group and aggregate data : {grouped_df}")

# Filter data
filtered_df = data_ops.filter_data(grouped_df, "salary_avg > 60000")
print(f"Filter data : {filtered_df}")

# Select specific columns
selected_df = data_ops.select_columns(filtered_df, ["category", "salary_avg"])

# Add new column
df_with_new_column = data_ops.add_column(selected_df, "salary_double_avg", expr("salary_avg * 2"))

# Show the final DataFrame
df_with_new_column.show()
