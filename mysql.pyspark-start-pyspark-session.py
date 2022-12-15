from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

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
#  loading the data from the MySQL table
wine_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "Wines") \
    .option("user", "root") \
    .option("password", "mysql") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# split the data into traing and testing sets
train_df , test_df  = wine_df.randomSplit([.6,.4],seed=12345)
predictors = ["fixed_acidity","volatile_acidity","citric_acid","residual_sugar","chlorides","free_so2",
              "total_so2","density","pH","sulphates","alcohol"]
# assemble the data into vectors
vec_assembler = VectorAssembler(inputCols=predictors,outputCol="features")
vec_train_df = vec_assembler.transform(train_df)
vec_train_df.select("features","is_red").show(10)

lr = LogisticRegression(labelCol="is_red",featuresCol="features")
# lr_model = lr.fit(vec_train_df)
# vec_test_df = vec_assembler.transform(test_df)
# predictions1 = lr_model.transform(vec_test_df)

pipeline = Pipeline(stages=[vec_assembler,lr])
pipeline_model = pipeline.fit(train_df)
predictions = pipeline_model.transform(test_df)

evaluator =  BinaryClassificationEvaluator(labelCol="is_red")
eval_result = evaluator.evaluate(predictions)
# eval_result1 = evaluator.evaluate(predictions1)
print("show predictions")
predictions.show()

print("use the `select` method to display the predicted and true labels")
predictions.select("prediction","is_red").show()

print("show evaluation results")
print(eval_result)

