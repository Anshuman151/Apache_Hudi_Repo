import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.functions import lit
from time import time 

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df1 = spark.read.format('parquet').load('s3://hivedatabucket/Source/Accounts/accounts.parquet')
#df1.count()
#df1.show(10)
df2= df1.withColumn('ts', lit(datetime.now()))
#df2.show(10)
hudi_options = {
    'hoodie.table.name': 'accounts_hudi',
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    'hoodie.datasource.write.recordkey.field': 'account_id',
    'hoodie.datasource.write.table.name': 'accounts_hudi',
    'hoodie.datasource.write.operation': 'bulk_insert',
    'hoodie.datasource.write.precombine.field': 'ts',
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "hudi_db",
    "hoodie.datasource.hive_sync.table": "accounts_hudi",
    "hoodie.datasource.hive_sync.mode" : "hms"
}
t1 = time()
df2.write.format('hudi').options(**hudi_options).mode('overwrite').save('s3://dbt-hudi-glue/Target/accounts_hudi')
t2 = time()
elapsed = t2 - t1
print('Elapsed time is %f seconds.' % elapsed)
job.commit()