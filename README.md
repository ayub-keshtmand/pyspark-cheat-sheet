# PySpark Cheat Sheet (Dataframes)

### Useful Links
* [PySpark SQL Cheat Sheet Python](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf)
* [Ultimate PySpark Cheat Sheet](https://towardsdatascience.com/ultimate-pyspark-cheat-sheet-7d3938d13421)

### General configuration/initialisation
```python
# SparkContext	connection to Spark to create RDDs
# SQLContext		connects to Spark to run SQL on data
# SparkSession	all-encompassing context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# create SparkSession instance
spark = SparkSession.builder.appName("moviedb").getOrCreate()

# create SparkContext
sc = SparkContext.getOrCreate()

# create SQLContext to access SQL query engine built on top of Spark
sqlContext = SQLContext(spark)
```

### Initialise spark session for dataframes
```python
from pyspark.sql import SparkSession
spark = SparkSession \
			.builder \
			.appName("something")
			.config("spark.some.config.option", "something")\
			.getOrCreate()

# stop spark session: spark.stop()
```

### Return tables in catalog
```python
spark.catalog.listTables()
```

### Create dataframe
```python
from pyspark.sql import Row
f1 = Row(title='Some Film', budget='12414', year=1992)
f2 = Row(title='Night World', budget='1231', year=1998)
films = [f1, f2]
df = spark.createDataFrame(films)

# create dataframe from pandas dataframe
df = spark.createDataFrame(df_pandas)

# create dataframe from table in catalog
df = spark.table("table_name")
```

### Read data
```python
# text
df = spark.read.text("customer.txt")

# csv
df = spark.read.csv("filename.csv")
# if contains headers then
df = spark.read.csv("filename.csv", header=True)
# alternatively:
df = spark \
		.read \
		.format('csv') \
		.options(header=True, inferSchema=True) \
		.load(csv_file_path)

# json
df = spark.read.json("customer.json")
df = spark.read.load("customer.json", format="json")

# parquet
df = spark.read.load("customer.parquet")

# delta lake
df = spark.read.format("delta").load(delta_lake_file_path)
```

### Write to file
```python
df.select("firstName", "city").write.save("data.parquet")
df.select("firstName", "age")\
  .write\
  .save("data.json", format="json")
```

### Append to file
```python
# parquet
df.write \
	.partitionBy('year') \
	.format('parquet') \
	.mode('overwrite') \ # 'append' to append to existing file
	.save(parquet_file_path)
```

### Write to database
```python
# hive table
df.write \
	.bucketBy(10, 'year') \
	.sortBy('avg_ratings') \
	.saveAsTable('films_bucketed')
```

### Inspect data
```python
df.dtypes # return df column names and data types
df.show() # display content
df.head() # return first n rows
df.first() # return first row
df.take(2) # return first n rows
df.schema # return schema
df.describe().show() # summary statistics
df.columns # return columns
df.count() # return row count
df.distinct().count() # return distinct row count
df.printSchema() # prints schema
df.explain() # print logical and physical plans
```

### Convert dataframes
```python
rdd = df.rdd # convert to RDD
df.toJSON() # convert into RDD of string
df.toPandas() # convert to pandas dataframe
```

### Drop duplicates
```python
df = df.dropDuplicates()
```

### Select columns
```python
df.select("firstName").show()
df.select("firstName", "lastName").show()
df.select(df.firstName, df.lastName).show()
from pyspark.sql.functions import col
df.select(col('train_id'), col('station')).show()

# show first name and age + 1
df.select( df["firstName"] , df["age"] + 1 ).show()

# show rows where age > 24
df.select( df["age"] > 24 ).show()

# show first name and boolean if age > 30
from pyspark.sql import functions as F
df.select(
	"firstName"
, F.when( df.age > 30 , 1 ).otherwise(0)
).show()

# select data using sql expression
df.selectExpr("firstName", "lastName", "age/2 as half_age")
```

### Substring
```python
# show first if in
df[ df.firstName.isin( "Jane" , "Boris" ) ].collect()

# show first name and last name if last name like x
df.select("firstName", df.lastName.like("Smith")).show()

# starts-with ends-with
df.select("firstName", df.lastName.startswith("Sm")).show()
df.select("firstName", df.lastName.endswith("th")).show()

# show age if between 22 and 24
df.select( df.age.between(22, 24) ).show()
```

### Replace strings
```python
df.select( regexp_replace("value", "Mr\.", "Mr") )
df.select( regexp_replace("value", "don\'t", "do not") )
```

### Split strings
```python
# split on space
df.select( split( "col" , "[ ]" ) )
```

### Create and add columns
```python
df = df.withColumn("new_col_name", df.col + 1) # e.g. add 1
df = df.withColumn("new_col", 1.4 * df.col("existing_col"))
df = df.withColumn("plane_age", df.year - df.plane_year)
df = df.withColumn("is_late", df.arrival_delay > 0)

df = df.withColumn("city", df.address.city) \
       .withColumn("postalCode", df.address.postalCode) \
       .withColumn("state", df.address.state)

# calculated field
avg_speed = (df.distance / df.air_time).alias("avg_speed")
speed1 = df.select("origin", "dest", avg_speed)
speed2 = df.selectExpr("origin", "dest", "distance/air_time" as avg_speed)

# create a column with the default value = "xyz"
df.withColumn("new_column", F.lit("xyz"))

# create a column with default value as null
df = df.withColumn("new_column",
	F.lit( None ).cast( StringType() )
)

# create a column based on criteria
df = df.withColumn("test_col", 
	F.when(F.col("avg_ratings") < 7, "OK") \
	.F.when(F.col("avg_ratings") < 8, "Good") \
	.otherwise("Great")).show()
)

# create a column using a UDF
def categorise(val):
	if val < 150:
		return "bucket_1"
	else:
		return "bucket_2"

my_udf = F.udf( categorise, StringType() )
df = df.withColumn("new_col", categorise("existing_col"))
```

### Rename columns
```python
# using withColumnRenamed
df = df.withColumnRenamed( "old_col_name", "new_col_name" )

# using selectExpr
df = df.selectExpr("existing_col_name as old1", "new_col_name as new1")

# using sparksql functions
from pyspark.sql.functions import col
df = df.select(
	col("existing_col_name").alias("existing_1")
,	col("new_col_name").alias("new_1")

# using SQL select statement
sqlContext.registerDataFrameAsTable(df, "df_table")
df = sql.Context.sql("""
	SELECT 
	existing_col_name AS existing_1
,	new_col_name AS new_1
	FROM df_table
""")
```

### Remove columns
```python
df = df.drop("address", "phoneNumber")
df = df.drop( df.address ).drop( df.phoneNumber )

# remove multiple columns in a list
drop_columns = ["this_column", "that_column"]
df.select([col for col in df.columns if column not in drop_columns])
```

### Cast columns
```python
df = df.withColumn( "col" , df.col.cast("new_type") )
df = df.withColumn("month", df.month.cast("integer"))
```

### Filter
```python
df.filter( df.age > 24 ).show()
df.filter( df["age"] > 24 ).show()
df.filter("age > 24").show()

filterA = df.origin == "SEA"
filterB = df.dest == "PDX"
df.filter(filterA).filter(filterB)
```

```python
# filter movies with avg_ratings > 7.5  and < 8.2
df.filter(
	( F.col('avg_ratings') > 7.5 ) &
	( F.col('avg_ratings') < 8.2 )
).show()
# or
df.filter( df.avg_ratings.between(7.5, 8.2) ).show()

# using where clause
df.where( F.lower( F.col('title') ).like("%ace%") ).show()
df.where("title like %ace%").show()
df.where(df.year != '1998').where( df.avg_ratings > 6 )

# dealing with nulls
df.where(df.budget.isNull()).show()
df.where(df.budget.isNotNull()).show()
```

### Remove rows
```python
non_blank_rows = df.where( length("word") > 0 )
```

### Group by
```python
df.groupBy("age").count().show()
df.groupBy().min("col").show()

# shortest flight from PDX in terms of distance
df.filter(df.origin=="PDX").groupBy().min("distance")

# longest flight from SEA in terms of air time
df.filter(df.origin=="SEA").groupBy().min("air_time")

# row count grouped by plane
df.groupby("plane").count().show()

# multiple aggregations
df.groupBy('year')\
	.agg( F.min('budget').alias('min_budget')		\
	 	, F.max('budget').alias('max_budget')		\
		, F.sum('revenue').alias('total_revenue')	\
		, F.avg('revenue').alias('avg_revenue')	\
		, F.mean('revenue').alias('mean_revenue')	\
		) \
	.sort( F.col('year').desc() ) \
	.show()
```

```python
df.groupBy("train_id").agg( {"time":"min"} )
df.groupBy("train_id").agg( {"time":"min", "time":"max"} )
```

### Pivot
```python
# year, revenue (2020, 45000 etc)
# pivot to convert year as column name and revenue as value
df.groupBy().pivot( 'year' ).agg( F.max('revenue') ).show()
```

### Sort
```python
df.sort( df.age.desc() ).collect()
df.sort( "age", ascending=False ).collect()
df.orderBy( ["age", "city"], ascending=[0,1] ).collect()
```

```python
# sort and orderBy can be used interchangeably in Spark
# except when it is used in Window functions
df.filter(df.year != '1998').sort( F.asc('year') )
df.filter(df.year != '1998').sort( F.desc('year') )
df.filter(df.year != '1998').sort( F.col('year').desc() )
df.filter(df.year != '1998').sort( F.col('year').asc() )

df.filter(df.year != '1998').orderBy( F.asc('year') )
df.filter(df.year != '1998').orderBy( F.desc('year') )
df.filter(df.year != '1998').orderBy( F.col('year').desc() )
df.filter(df.year != '1998').orderBy( F.col('year').asc() )
```

### Join
```python
# full outer join
df1.join(df2, on='title', how='full')
df1.join(df2, on='title', how='outer')
df1.join(df2, df1.title == df2.title, how='outer')

# left join
df1.join(df2, on='title', how='left')

# cross join
df1.join(df2)

# semi/anti left joins
df1.join(df2, on=['title'], how='left_anti')
df1.join(df2, on=['title'], how='left_semi')
```

### Window functions
```python
# sql version
query = """
	SELECT *
	, ROW_NUMBER() OVER(PARTITION BY train_id ORDER BY time) AS id
	FROM schedule
"""
spark.sql(query).show()

# dot notation version
from pyspark.sql import Window
from pyspark.sql import row_number
df.withColumn("id",
	row_number().over(
		Window.partitionBy("train_id").orderBy("time") )
)
```

```python
# using WindowSpec
window = Window.partitionBy("train_id").orderBy("time")
dfx = df.withColumn( "next", lead("time",1).over(window) )
```

```python
# rank films by revenue partitioned by year in desc order
window = Window.partitionBy("year").orderBy("revenue").desc()
df.select( "title" , "year" , F.rank().over(window) )
```

### Nulls
```python
df.na.fill(50) # replace nulls
df.na.drop() # remove nulls
df.na.replace(10, 20) # replace 10 and 20

df.filter("col1 IS NOT NULL and col2 IS NOT NULL")

# return subset of null/notnull data
df.where( df.budget.isNull() ).show()
df.where( df.budget.isNotNull() ).show()
```

### Add row id column
```python
df.select("col", monotonically_increasing_id().alias("id"))
```

### Case style statement
```python
df.withColumn("title",
	when( df.id < 25000 , "Preface" )
 .when( df.id < 50000 , "Chapter 1" )
 .when( df.id < 75000 , "Chapter 2" )
 .otherwise( "Chapter 3" )
)
```

### Partitioning data
```python
df.withColumn("part",
	when( df.id < 25000 , 0 )
 .when( df.id < 50000 , 1 )
 .when( df.id < 75000 , 2 )
 .otherwise( 3 )
)
df2 = df.repartition( 4 , 'part' )
# 1st arg: number of partitions
# 2nd arg: put rows having the same 'part' column value into the same partition

print(df2.rdd.getNumPartitions()) # 4
```

### Tokenise words in array/list
```python
# split the "clause" column into a column called "words"
split_df = clauses_df.select(
	split( "clause" , " " ).alias( "words" )
)

exploded_df = split_df.select(
	explode( "words" ).alias( "word" )
)
```
