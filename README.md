# Spark connect to MySQL(BO)

PySpark

![obj-2128.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/obj-2128.png)

[使用 Docker 快速建置 PySpark 環境](https://medium.com/@bee811101/%E4%BD%BF%E7%94%A8-docker-%E5%BF%AB%E9%80%9F%E5%BB%BA%E7%BD%AE-pyspark-%E7%92%B0%E5%A2%83-657f9d8bff3a)

[PySpark + MySQL Tutorial](https://towardsdatascience.com/pyspark-mysql-tutorial-fa3f7c26dc7)

[20200727 Reading and writing data from/to MySQL using Apache Spark](https://www.youtube.com/watch?v=7PZyUs4f3Bo&ab_channel=DataEngineeringTutorial)

[零經驗也可的 PySpark 教學 - 初體驗](https://myapollo.com.tw/zh-tw/pyspark-1/)

環境 

linux ubuntu 22.04

openjdk-8-jre-headless

PySpark 需要 JAVA version 1.8.0 以上 以及 Pytohn 3.6以上

Apache Spark 使用 java runtime 而 PySpark 介面用 python 開發和運算

mysql 部分使用docker-compose 創建一個mysql container 設定port 13306:3306

```docker
db:
    image: mysql
    container_name: mysql_test
    restart: always
    environment:
     
      MYSQL_ROOT_PASSWORD: mysql
     
    ports:
      - "13306:3306"
```

 也可於本機安裝 mysql

```bash
sudo apt install mysql-server
```

![螢幕擷取畫面 2022-12-14 115143.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_115143.png)

安裝  pySpark

 需事先安裝好 python3-pip (sudo apt install python3-pip)

```bash
pip install pyspark
```

同時也會安裝 py4j , spark to mysql 的 JDBC driver

安裝java jdk

```bash
sudo apt install openjdk-8-jre-headless
```

(ChatGPT 說 8版的穩定與PySpark更相容)

因為 spark 底層用 JAVA 撰寫，因此需要安裝 JAVA driver 讓 Spark 可以連到MySQL (MySQL JDBC connector)

至  [MySQL Community Downloads](https://dev.mysql.com/downloads/connector/j/)下載安裝，下載對應你環境的package

![螢幕擷取畫面 2022-12-14 120152.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_120152.png)

ubuntu 上下載後可執行，或使用 package manager 安裝

```bash
sudo dpkg -i mysql-connector-j_8.0.31-1ubuntu22.04_all.deb
```

安裝後路徑存在:

![螢幕擷取畫面 2022-12-14 120837.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_120837.png)

本次使用PySpark 對紅酒資料集建立 ML model作為串接示範應用

(資料來源 UCI  [wine quality dataset](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/))

先將要操作的csv檔案放在相同層級資料夾中 (這樣方便操作，也可以從別的地方載入)

![螢幕擷取畫面 2022-12-14 171458.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_171458.png)

創建一個檔案`mysl-pyspark-load-wine.py` 在其中，

```python
import pandas as pd

red_wines = pd.read_csv("winequality-red.csv", sep=";")
red_wines["is_red"] = 1
white_wines = pd.read_csv("winequality-white.csv", sep=";")
white_wines["is_red"] = 0
all_wines = pd.concat([red_wines, white_wines])
print(all_wines)
```

(如果  沒有pandas 就pip install pandas)

執行(python3 `mysl-pyspark-load-wine.py`)之後可以看到csv  讀取到pandas  中

![螢幕擷取畫面 2022-12-14 171758.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_171758.png)

接著使用 python 連線到mysql 使用 mysql-connector-python 套件，沒有就安裝

記得先設定好 mysql (可安裝在電腦環境中或使用 docker環境)

- host: 本機或docker本機使用localhost
- port: 除非有在docker 特別指定，不然多為3306
- user: 你設定的user name,  有root 但是通常下載後都會新增一個使用者
- password: 使用者的密碼

如果遇到驗證[sha2_password](https://stackoverflow.com/questions/50557234/authentication-plugin-caching-sha2-password-is-not-supported)問題

測試連線成功之後，用python 建立一個table 

```python
import mysql.connector

db_connection = mysql.connector.connect(host='localhost',port='3306',user="root", password="mysql")
db_cursor = db_connection.cursor()
db_cursor.execute("CREATE DATABASE TestDB;")
db_cursor.execute("USE TestDB;")

db_cursor.execute("CREATE TABLE Wines(fixed_acidity FLOAT, volatile_acidity FLOAT, \
                   citric_acid FLOAT, residual_sugar FLOAT, chlorides FLOAT, \
                   free_so2 FLOAT, total_so2 FLOAT, density FLOAT, pH FLOAT, \
                   sulphates FLOAT, alcohol FLOAT, quality INT, is_red INT);")
```

建好 table 之後，將csv的值寫入MySQL 

```python
import pandas as pd 
import mysql.connector

 # 從csv 拿取資料
red_wines = pd.read_csv("winequality-red.csv", sep=";")
red_wines["is_red"] = 1

white_wines = pd.read_csv("winequality-white.csv", sep=";")
white_wines["is_red"] = 0

all_wines = pd.concat([red_wines, white_wines])
# ------------------------連線MSQL
db_connection = mysql.connector.connect(host='localhost',port = "13306",
																user = "root", password = "mysql",**database = "TestDB"**)
db_cursor = db_connection.cursor()

# 將資料每行內容包在括號內並用逗號分開
# MySQL 可以多行載入

wine_tuples = list(all_wines.itertuples(index = False,name = None))
wine_tuples_str = ",".join(["("+",".join([str(w) for w in wt])+")" for wt in wine_tuples])

db_cursor.execute("INSERT INTO Wines(fixed_acidity, volatile_acidity, citric_acid,\
                   residual_sugar, chlorides, free_so2, total_so2, density, pH,\
                   sulphates, alcohol, quality, is_red) VALUES " + wine_tuples_str + ";")
db_cursor.execute("FLUSH TABLES;")
```

將剛才寫入MySQL 的資料 使用PySpark SQL  從 mysql拿出來

```python
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
tableDF.show()
```

從MySQL  取出資料集，並使用 紅白酒的資料集來預測是否為紅酒(酸度、酒精、果酸等)

使用邏輯回歸模型(logistic regression)來判斷是否為紅白酒 

前面有先針對，紅白酒標註是否為紅酒 [is_red]

```python
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
```

訓練過程解釋：

[randonSplit](https://blog.csdn.net/dangsh_/article/details/80193051) 用來生成多個隨機的dataset，這邊給定兩個比例用來生成典型的 train / test 兩個sets

predictors 是用來訓練的預測變數

```python
train_df , test_df  = wine_df.randomSplit([.6,.4],seed=12345)
predictors = ["fixed_acidity","volatile_acidity","citric_acid","residual_sugar","chlorides","free_so2",
              "total_so2","density","pH","sulphates","alcohol"]
```

VectorAssembler  輸入 predictors, 輸出 feature vector，將預測變數組裝成single vector 好導入邏輯回歸模型

![螢幕擷取畫面 2022-12-15 150320.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-15_150320.png)

針對 features 和 目標 is_red 進行訓練，就是 LogisticRegression後面接的參數

```python
# assemble the data into vectors
vec_assembler = VectorAssembler(inputCols=predictors,outputCol="features")
vec_train_df = vec_assembler.transform(train_df)
vec_train_df.select("features","is_red").show(10)

lr = LogisticRegression(labelCol="is_red",featuresCol="features")
```

PySpark 可以用Pipeline 將 vector assembler 和 LogisticRegression的 model fit / transform 步驟連接起來變成一個 pipeline operation

```python
pipeline = Pipeline(stages=[vec_assembler,lr])
pipeline_model = pipeline.fit(train_df)
predictions = pipeline_model.transform(test_df)
```

最後用 `BinaryClassificationEvaluator`計算[Receiver operator characteristic (ROC)](https://ithelp.ithome.com.tw/articles/10229049) 曲線下的面積，eval_result 是一個數值用來評估 model performance 越高代表越準確

中間可以把預測的dataframe 印出來看

```python
evaluator =  BinaryClassificationEvaluator(labelCol="is_red")
eval_result = evaluator.evaluate(predictions)
# eval_result1 = evaluator.evaluate(predictions1)
print("show predictions")
predictions.show()

# 只印出預測的值和真實數據看差異
print("use the `select` method to display the predicted and true labels")
predictions.select("prediction","is_red").show()

print("show evaluation results")
print(eval_result) # 0.9960093059725827
```

![螢幕擷取畫面 2022-12-15 154745.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-15_154745.png)

![螢幕擷取畫面 2022-12-15 154843.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-15_154843.png)

![螢幕擷取畫面 2022-12-15 155045.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-15_155045.png)

過程中如果遇到 BLAS (Basic Linear Algebra Subprograms) 的問題

```python
22/12/15 11:46:09 WARN InstanceBuilder$NativeBLAS: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
22/12/15 11:46:09 WARN InstanceBuilder$NativeBLAS: Failed to load implementation from:dev.ludovic.netlib.blas.ForeignLinkerBLAS
```

代表系統中沒有底層的線性代數向量矩陣算數的library

需要在系統中安裝適當的BLAS library 好讓spark 可以抓到

如果是 linux ubuntu 可以透過`apt` 安裝

```bash
sudo apt install libopenblas-base
```

如果安裝後，Spark 沒有找到的話需要把路徑export 成環境變數

![螢幕擷取畫面 2022-12-15 144819.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-15_144819.png)

接著可以把BinaryClassificationEvaluator 評估的結果印出來看

```python
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

print("show evaluation results")
print(eval_result)
```

## 使用 container 的方式安裝 Apache Spark / pySpark 與 MySQL

![螢幕擷取畫面 2022-12-14 141358.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_141358.png)

```bash
	docker-compose up 
```

拉取 image之後就可以看到  mysql 和  pySpark啟動

![螢幕擷取畫面 2022-12-14 141604.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_141604.png)

在瀏覽器打開連結，就可以看到畫面

![螢幕擷取畫面 2022-12-14 141703.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_141703.png)

這樣就完成 Apache Spark 與  PySpark架設

請在啟動  docker-compose  時，確認好資料集路徑 (我放在同一個專案層級底下方便使用)

![螢幕擷取畫面 2022-12-14 143643.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_143643.png)

![螢幕擷取畫面 2022-12-14 143622.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_143622.png)

****Jupyter Docker Stacks - pyspark-notebook****

開啟 notebook 進行

接著新增一個 Jupyter notebook 的頁面後，就來一步一步稍微體驗一下 Spark！

**以下章節的範例程式碼請在 Jupyter notebook 中輸入執行**

- 要操作 spark  一切功能都需要使用 SparkSession

> The entry point into all functionality in Spark is the [SparkSession](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html) class.
> 

建立一個 notebook cell

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

建立會話之後可以從 spark 載入資料集到記憶體中計算

```python
df = spark.read\
        .option('header',True)\
        .option('escape','"')\
        .option('inferSchema', True)\
        .csv('winequality-red.csv')
df.printSchema()
```

上述範例的 `.option('header', True)`
 代表將 CSV 檔案的第 1 行視為欄位名稱；而 `.option('escape', '"')`
 則是設定 CSV 欄位值如果含有 `"`
 雙引號時要如何逸出(escape), 例如欄位值為 `我是一名"程式"設計師`
 的話，經過 `"`
 逸出之後就會變成 `我是""程式""設計師`
 執行後，`printSchema` 會把資料結構印出來看:

![螢幕擷取畫面 2022-12-14 144508.png](Spark%20connect%20to%20MySQL(BO)%20e60b19fa927241fca7eec65f40807fbe/%25E8%259E%25A2%25E5%25B9%2595%25E6%2593%25B7%25E5%258F%2596%25E7%2595%25AB%25E9%259D%25A2_2022-12-14_144508.png)