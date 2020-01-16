# /home/shubham/sp_sc/crime/type_of_crime_catg1-2.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col #for changing column names
import pyspark.sql.functions as f #	spark functions


print("					----- Script execution starts -----\n\n")

spark = SparkSession \
.builder \
.appName("Total count of types of crimes") \
.config("spark.some.config.option", "Dataframe") \
.getOrCreate()
	
crime_df=spark.read.csv("file:/home/shubham/Crime.csv", header="True")	
#type_of_crime=crime_df.groupby("Crime Name1").count()




prepare_victim_tbl=crime_df.groupby('Crime Name1').agg(f.count('Crime Name2'))

prepare_victim_tbl_resut=prepare_victim_tbl.select(col("Crime Name1").alias("Category_A_crimes"), col("count(Crime Name2)").alias("Total_Category_of_Category_B_crimes"))
prepare_victim_tbl_resut.show()




#type_of_crime_result = type_of_crime.select(col("Crime Name1").alias("Type_of_crimes"), col("count").alias("Total_number_of_cases_registered"))

#type_of_crime_result.show()


print("				----- Script execution ends -----\n\n")
