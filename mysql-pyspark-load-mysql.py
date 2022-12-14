import mysql.connector

db_connection = mysql.connector.connect(host='localhost',port = "13306",user = "root", password = "mysql")
db_cursor = db_connection.cursor()
db_cursor.execute("CREATE DATABASE TestDB;")
db_cursor.execute("USE TestDB;")
db_cursor.execute("CREATE TABLE Wines(fixed_acidity FLOAT, volatile_acidity FLOAT, \
                   citric_acid FLOAT, residual_sugar FLOAT, chlorides FLOAT, \
                   free_so2 FLOAT, total_so2 FLOAT, density FLOAT, pH FLOAT, \
                   sulphates FLOAT, alcohol FLOAT, quality INT, is_red INT);"
)