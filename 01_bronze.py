from pyspark.sql import functions as F
PATH = '/Volumes/mvp_seguranca/bronze/arquivos/acidentes_mvp_anonimizado.csv'
df = (spark.read.option('header', True).option('inferSchema', False).csv(PATH)
      .withColumn('_ingestion_ts', F.current_timestamp())
      .withColumn('_source_file', F.input_file_name()))
df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('mvp_seguranca.bronze.acidentes_raw')
display(spark.table('mvp_seguranca.bronze.acidentes_raw'))
