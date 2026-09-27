from pyspark.sql import functions as F
import re, unicodedata
def snake(s):
    s=unicodedata.normalize('NFKD',s).encode('ascii','ignore').decode()
    return re.sub(r'_+','_',re.sub(r'[^a-zA-Z0-9]+','_',s)).strip('_').lower()
df=spark.table('mvp_seguranca.bronze.acidentes_raw')
for old in df.columns: df=df.withColumnRenamed(old,snake(old))
for c,t in df.dtypes:
    if t=='string' and not c.startswith('_'):
        df=df.withColumn(c,F.when(F.trim(F.col(c))=='',None).otherwise(F.trim(F.col(c))))
for c in ['ano','mes','dia','idade']: df=df.withColumn(c,F.col(c).cast('int'))
for c in ['dias_perdidos','dias_debitados']: df=df.withColumn(c,F.coalesce(F.regexp_replace(c,',','.').cast('double'),F.lit(0.0)))
df=df.withColumn('classificacao',F.regexp_replace('classificacao','Quase acidente','Quase Acidente'))
df=df.withColumn('classificacao',F.regexp_replace('classificacao','Desvio Critico','Desvio Crítico'))
df=df.withColumn('compromissos_regra_de_ouro',F.regexp_replace('compromissos_regra_de_ouro',r'\s*-\s*',' - '))
df=df.withColumn('data_evento',F.make_date('ano','mes','dia'))
l=F.lower(F.col('classificacao'))
df=df.withColumn('tipo_evento',F.when(l.contains('fatal'),'Fatalidade').when(l.contains('quase'),'Quase Acidente').when(l.contains('desvio'),'Desvio Crítico').when(l.contains('trajeto'),'Acidente de Trajeto').when(l.contains('típico')|l.contains('tipico'),'Acidente Típico').when(l.contains('doença'),'Doença Ocupacional').otherwise('Outro')).dropDuplicates()
df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('mvp_seguranca.silver.acidentes_limpos')
display(df)
