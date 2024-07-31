from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, mean, stddev, year, month, dayofmonth, hour, log1p, mean as _mean
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, DecimalType, TimestampType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from IPython.display import Image, display


class EDA:
    def __init__(self, df):
        self.df = df

    def show_basic_info(self):
        print("Schema of the DataFrame:")
        self.df.printSchema()
        
        print("First few records:")
        self.df.show(5)
        
        print("Basic statistics:")
        numeric_columns = [field.name for field in self.df.schema.fields if isinstance(field.dataType, (IntegerType, FloatType, DoubleType, DecimalType))]
        self.df.select(numeric_columns).describe().show()
        
        print("Count of missing values per column:")
        missing_counts = self.df.select([count(when(col(c).isNull(), c)).alias(c) for c in self.df.columns])
        missing_counts.show()
        
        print("Data types of columns:")
        print(self.df.dtypes)
        
        print("Count distinct values in each column:")
        distinct_counts = self.df.select([count(col(c)).alias(c) for c in self.df.columns])
        distinct_counts.show()
        
        print(f"Number of rows: {self.df.count()}")
        print(f"Number of columns: {len(self.df.columns)}")

    def show_value_counts(self, categorical_columns):
        print("Value counts for categorical columns:")
        for col_name in categorical_columns:
            self.df.groupBy(col_name).count().show()

    def handle_missing_values(self, numeric_columns):
        for col_name in numeric_columns:
            mean_value = self.df.select(mean(col(col_name))).collect()[0][0]
            self.df = self.df.na.fill({col_name: float(mean_value)})
        return self.df

    def show_skewness_kurtosis(self, numeric_columns):
        print("Skewness and Kurtosis for numerical columns:")
        for col_name in numeric_columns:
            skewness = self.df.selectExpr(f'skewness({col_name})').collect()[0][0]
            kurtosis = self.df.selectExpr(f'kurtosis({col_name})').collect()[0][0]
            print(f'{col_name} - Skewness: {skewness}, Kurtosis: {kurtosis}')

    def handle_outliers(self, numeric_columns):
        for col_name in numeric_columns:
            mean_value = float(self.df.select(mean(col(col_name))).collect()[0][0])
            std_dev = float(self.df.select(stddev(col(col_name))).collect()[0][0])
            self.df = self.df.filter((col(col_name) >= mean_value - 3 * std_dev) & (col(col_name) <= mean_value + 3 * std_dev))
        return self.df

    def convert_to_pandas(self):
        return self.df.toPandas()

    def plot_correlation_heatmap(self, pandas_df, numeric_columns):
        correlation_matrix = pandas_df[numeric_columns].corr()
        plt.figure(figsize=(12, 10))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
        plt.title('Correlation Heatmap')
        plt.savefig('correlation_heatmap.png')
        plt.close()
        display(Image(filename='correlation_heatmap.png'))

    def plot_distributions(self, pandas_df, numeric_columns):
        for col_name in numeric_columns:
            plt.figure(figsize=(10, 6))
            sns.histplot(pandas_df[col_name], kde=True)
            plt.title(f'Distribution of {col_name}')
            plt.savefig(f'distribution_{col_name}.png')
            plt.close()
            display(Image(filename=f'distribution_{col_name}.png'))

    def plot_box_plots(self, pandas_df, numeric_columns):
        for col_name in numeric_columns:
            plt.figure(figsize=(10, 6))
            sns.boxplot(x=pandas_df[col_name])
            plt.title(f'Box plot of {col_name}')
            plt.savefig(f'boxplot_{col_name}.png')
            plt.close()
            display(Image(filename=f'boxplot_{col_name}.png'))

    def feature_engineering(self):
        # Creating a new feature from an existing column (extracting year, month, day from a timestamp)
        timestamp_columns = [field.name for field in self.df.schema.fields if isinstance(field.dataType, TimestampType)]
        for col_name in timestamp_columns:
            self.df = self.df.withColumn(f'{col_name}_year', year(col(col_name)))
            self.df = self.df.withColumn(f'{col_name}_month', month(col(col_name)))
            self.df = self.df.withColumn(f'{col_name}_day', dayofmonth(col(col_name)))
            self.df = self.df.withColumn(f'{col_name}_hour', hour(col(col_name)))


        # Encoding categorical columns
        categorical_columns = [field.name for field in self.df.schema.fields if isinstance(field.dataType, StringType)]
        for col_name in categorical_columns:
            distinct_vals = self.df.select(col_name).distinct().collect()
            for val in distinct_vals:
                val_str = str(val[0]).replace(' ', '_')
                self.df = self.df.withColumn(f'{col_name}_{val_str}', when(col(col_name) == val[0], 1).otherwise(0))

        return self.df
    

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

# Load data from MySQL table into DataFrame
table_name = "dq_chk"
df = spark.read.jdbc(url, table_name, properties=properties)


eda = EDA(df)
    
eda.show_basic_info()
    
numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, FloatType, DoubleType, DecimalType))]
categorical_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    
# eda.show_value_counts(categorical_columns)
# df = eda.handle_missing_values(numeric_columns)
# eda.show_skewness_kurtosis(numeric_columns)
# df = eda.handle_outliers(numeric_columns)
    
# pandas_df = eda.convert_to_pandas()
# eda.plot_correlation_heatmap(pandas_df, numeric_columns)
# eda.plot_distributions(pandas_df, numeric_columns)
# eda.plot_box_plots(pandas_df, numeric_columns)

# Feature engineering
df_fe = eda.feature_engineering()
df_fe.show(5)

# Write DataFrame to Azure SQL Database
df_fe.write.jdbc(url=url, table="transformed_table", mode="overwrite", properties=properties)


# Stop the Spark session
spark.stop()
