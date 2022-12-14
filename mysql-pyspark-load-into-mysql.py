import pandas as pd 
import mysql.connector

red_wines = pd.read_csv("winequality-red.csv", sep=";")
red_wines["is_red"] = 1

white_wines = pd.read_csv("winequality-white.csv", sep=";")
white_wines["is_red"] = 0

all_wines = pd.concat([red_wines, white_wines])
# ------------------------
db_connection = mysql.connector.connect(host='localhost',port = "13306",user = "root", password = "mysql",database = "TestDB")
db_cursor = db_connection.cursor()

# ----------------------------------------------------------------
wine_tuples = list(all_wines.itertuples(index = False,name = None))
wine_tuples_str = ",".join(["("+",".join([str(w) for w in wt])+")" for wt in wine_tuples])

db_cursor.execute("INSERT INTO Wines(fixed_acidity, volatile_acidity, citric_acid,\
                   residual_sugar, chlorides, free_so2, total_so2, density, pH,\
                   sulphates, alcohol, quality, is_red) VALUES " + wine_tuples_str + ";")
db_cursor.execute("FLUSH TABLES;")
