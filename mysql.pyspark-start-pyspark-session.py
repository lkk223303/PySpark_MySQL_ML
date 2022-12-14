from pyspark.sql import SparkSession
from pyspark.sql import Row

# Set the jdbc url 
jdbc_url = "jdbc:mysql://localhost:13306/TestDB"

# Create a SparkSession
spark  = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-j-8.0.31.jar") \
                .master("local").appName("PySpark_MySQL_test").getOrCreate()

connectionProperties = {
    "user": "root",
    "password": "mysql",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read the data from the MySQL table
tableDF = spark.read \
    .jdbc(url = jdbc_url,table= "Wines",properties= connectionProperties,lowerBound=10,upperBound=100,numPartitions=10)


# Show the data in the DataFrame
# tableDF.show()

wine_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "Wines") \
    .option("user", "root") \
    .option("password", "mysql") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
