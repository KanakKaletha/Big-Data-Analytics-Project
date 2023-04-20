import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.sql.types import StringType, IntegerType, StructField, StructType



# Initialize SparkSession
spark = SparkSession.builder.appName("S3DataFrame").getOrCreate()

# Create dataframe from S3 bucket data
df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://puttriggerlambda/unclean_data/amazon_prime_titles.csv/amazon_prime_titles.csv")

df2=spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://puttriggerlambda/unclean_data/disney_plus_shows.csv/disney_plus_shows.csv")

df3=spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://puttriggerlambda/unclean_data/hulu_titles.csv/hulu_titles.csv")

df4=spark.read.format("csv").option("header","true").option("inferSchame","true").load("s3://puttriggerlambda/unclean_data/netflix_titles.csv/netflix_titles.csv")

df5=spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://puttriggerlambda/unclean_data/rotten_tomatoes_critic_reviews.csv/rotten_tomatoes_critic_reviews.csv")

df6=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/rotten_tomatoes_movies.csv/rotten_tomatoes_movies.csv")

df7=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/IMDb/genome-scores.csv")

df8=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/IMDb/genome-tags.csv")

df9=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/IMDb/links.csv")

df10=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/IMDb/movies.csv")

df11=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/IMDb/ratings.csv")

df12=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/IMDb/tags.csv")

df13=spark.read.format("csv").option("header","true").option("inferSchaema","true").load("s3://puttriggerlambda/unclean_data/youtube/youtube.csv")





# fill null values in string columns with a default value (e.g. "unknown")
df1=df1.fillna("NA", subset=[c for c in df1.columns if df1.schema[c].dataType==StringType()])
df1=df1.drop(col("country"))

df2=df2.fillna("NA",subset=[c for c in df2.columns if df2.schema[c].dataType==StringType()])
df2=df2.drop(col("country"))

df3=df3.fillna("NA",subset=[c for c in df3.columns if df3.schema[c].dataType==StringType()])
df3=df3.drop(col("country"))

df4=df4.fillna("NA",subset=[c for c in df4.columns if df4.schema[c].dataType==StringType()] )

df5=df5.fillna("NA",subset=[c for c in df5.columns if df5.schema[c].dataType==StringType()] )

df6=df6.fillna("NA",subset=[c for c in df6.columns if df6.schema[c].dataType==StringType()])

df7=df7.fillna("NA",subset=[c for c in df7.columns if df7.schema[c].dataType==StringType()])

df8=df8.fillna("NA",subset=[c for c in df8.columns if df8.schema[c].dataType==StringType()])

df9=df9.fillna("NA",subset=[c for c in df9.columns if df9.schema[c].dataType==StringType()])

df10=df10.fillna("NA",subset=[c for c in df10.columns if df10.schema[c].dataType==StringType()])

df11=df11.fillna("NA",subset=[c for c in df11.columns if df11.schema[c].dataType==StringType()])

df12=df12.fillna("NA",subset=[c for c in df12.columns if df12.schema[c].dataType==StringType()])

df13=df13.fillna("NA",subset=[c for c in df13.columns if df13.schema[c].dataType==StringType()])




#df = df.fillna({"column1": "NA", "column2": "NA"})

#Write Clean Data to S3
df1.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/amazon_prime_titles.parquet")

df2.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/disney_plus_shows.parquet")

df3.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/hulu_titles.parquet")

df4.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/netflix_titles.parquet")

#df5.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/rotten_tomatoes_critic_reviews.parquet")

df6.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/rotten_tomatoes_movies.parquet")

df7.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/imdb_genome-scores.parquet")

df8.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/imdb_genome-tags.parquet")

df9.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/imdb_links.parquet")

df10.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/imdb_movies.parquet")

df11.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/imdb_ratings.parquet")

df12.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/imdb_tags.parquet")

df13.coalesce(1).write.format("parquet").mode("overwrite").save("s3://puttriggerlambda/Output_emr/youtube.parquet")

df5.coalesce(1).write.parquet("s3://dataforset/presenttation_try/")

spark.stop()
