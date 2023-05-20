# Import's
from pyspark.sql import HiveContext
from pyspark.sql.functions import from_json, col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Config Spark

# Code Gereneral

df_category = spark.read.option("multiline", "true").format("json").load("../../datasets/youtube/BR_category_id.json")
#Parse das categorias
df_category = df_category_br.select(F.explode('items').alias('categoria'))\
                            .select('categoria.id', 'categoria.snippet.title')

df_category = df_category.withColumnRenamed("id", "category_id")\
                         .withColumnRenamed("title", "category_title")

#Carregando dados de trending de vídeos
df_br = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/BR_youtube_trending_data.csv", header=True)
df_br = df_br.withColumn("country", lit(str("BR")))
df_ca = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/CA_youtube_trending_data.csv", header=True)
df_ca = df_ca.withColumn("country", lit(str("CA")))
df_de = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/DE_youtube_trending_data.csv", header=True)
df_de = df_de.withColumn("country", lit(str("DE")))
df_fr = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/FR_youtube_trending_data.csv", header=True)
df_fr = df_fr.withColumn("country", lit(str("FR")))
df_gb = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/GB_youtube_trending_data.csv", header=True)
df_gb = df_gb.withColumn("country", lit(str("GB")))
df_in = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/IN_youtube_trending_data.csv", header=True)
df_in = df_in.withColumn("country", lit(str("IN")))
df_jp = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/JP_youtube_trending_data.csv", header=True)
df_jp = df_jp.withColumn("country", lit(str("JP")))
df_kr = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/KR_youtube_trending_data.csv", header=True)
df_kr = df_kr.withColumn("country", lit(str("KR")))
df_mx = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/MX_youtube_trending_data.csv", header=True)
df_mx = df_mx.withColumn("country", lit(str("MX")))
df_ru = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/RU_youtube_trending_data.csv", header=True)
df_ru = df_ru.withColumn("country", lit(str("RU")))
df_us = spark.read.options(header='True', delimiter=',').csv("../../datasets/youtube/US_youtube_trending_data.csv", header=True)
df_us = df_us.withColumn("country", lit(str("US")))
df_final = df_br.union(df_ca)\
                .union(df_de)\
                .union(df_fr)\
                .union(df_gb)\
                .union(df_in)\
                .union(df_jp)\
                .union(df_kr)\
                .union(df_mx)\
                .union(df_ru)\
                .union(df_us)

# Foi necessário fazer o join antes para contornar os dados de description que quebram o csv por 
# conta de "vírgulas" no meio do texto
df_final = df_final.join(df_category, df_final.categoryId ==  df_category.category_id, "inner")

#Dataframe agrupado por país/categoria gerando agregações de:
#soma de likes
#soma de dislikes
#soma de views
#soma de comments
#média de likes
#média de dislikes
#média de views
#média de comments
df_final_country = df_final.groupBy("country", "categoryId") \
                    .agg(F.sum("likes").cast(LongType()).alias("sum_likes"), \
                         F.sum("dislikes").cast(LongType()).alias("sum_dislikes"), \
                         F.sum("view_count").cast(LongType()).alias("sum_views"), \
                         F.sum("comment_count").cast(LongType()).alias("sum_comments"), \
                         F.avg("likes").cast(DecimalType(18, 2)).alias("avg_likes"), \
                         F.avg("dislikes").cast(DecimalType(18, 2)).alias("avg_dislikes"), \
                         F.avg("view_count").cast(DecimalType(18, 2)).alias("avg_views"), \
                         F.avg("comment_count").cast(DecimalType(18, 2)).alias("avg_comments") \
                        )
#Dataframe agrupado por categoria gerando agregações de:
#soma de likes
#soma de dislikes
#soma de views
#soma de comments
#média de likes
#média de dislikes
#média de views
#média de comments
df_final_category = df_final.groupBy("categoryId") \
                    .agg(F.sum("likes").cast(LongType()).alias("sum_likes"), \
                         F.sum("dislikes").cast(LongType()).alias("sum_dislikes"), \
                         F.sum("view_count").cast(LongType()).alias("sum_views"), \
                         F.sum("comment_count").cast(LongType()).alias("sum_comments"), \
                         F.avg("likes").cast(DecimalType(18, 2)).alias("avg_likes"), \
                         F.avg("dislikes").cast(DecimalType(18, 2)).alias("avg_dislikes"), \
                         F.avg("view_count").cast(DecimalType(18, 2)).alias("avg_views"), \
                         F.avg("comment_count").cast(DecimalType(18, 2)).alias("avg_comments") \
                        )

#Top categorias por indicadores por país
df_final_country = df_final_country.join(df_category, df_final.categoryId ==  df_category.category_id, "inner")

ranked_country =  df_final_country \
            .withColumn("rank_likes", rank().over(Window.partitionBy("country").orderBy(desc("sum_likes")))) \
            .withColumn("rank_deslikes", rank().over(Window.partitionBy("categoryId").orderBy(desc("sum_dislikes")))) \
            .withColumn("rank_views", rank().over(Window.partitionBy("categoryId").orderBy(desc("sum_views")))) \
            .withColumn("rank_comments", rank().over(Window.partitionBy("categoryId").orderBy(desc("sum_comments")))) \
        .orderBy("country").show()

#Top indicadores por categorias
df_final_category = df_final_category.join(df_category, df_final.categoryId ==  df_category.category_id, "inner")

ranked_category =  df_final_category.withColumn("partition", lit("category"))
ranked_category = ranked_category.withColumn("rank_likes", rank().over(Window.partitionBy("partition").orderBy(desc("sum_likes")))) \
             .withColumn("rank_deslikes", rank().over(Window.partitionBy("partition").orderBy(desc("sum_dislikes")))) \
             .withColumn("rank_views", rank().over(Window.partitionBy("partition").orderBy(desc("sum_views")))) \
             .withColumn("rank_comments", rank().over(Window.partitionBy("partition").orderBy(desc("sum_comments")))) \
         .orderBy("categoryId").show()