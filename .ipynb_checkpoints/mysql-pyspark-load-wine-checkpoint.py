import pandas as pd 

red_wines = pd.read_csv("winequality-red.csv", sep=";")
red_wines["is_red"] = 1

white_wines = pd.read_csv("winequality-white.csv", sep=";")
white_wines["is_red"] = 0

all_wines = pd.concat([red_wines, white_wines])
print(all_wines)