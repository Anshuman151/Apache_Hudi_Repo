import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.functions import lit
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session.builder \
        .config('spark.jars.packages','org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1') \
        .config('spark.serializer','org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.extensions','org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
        .config('spark.sql.catalog.glue_catalog','org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
        .config('spark.kryo.registrator','org.apache.spark.HoodieSparkKryoRegistrar') \
        .config('spark.sql.hive.convertMetastoreParquet','false') \
        .getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df1 = spark.read.format('parquet').load('s3://hivedatabucket/Source/Accounts/accounts.parquet')
#df1.count()
#df1.show(10)
df2= df1.withColumn('ts', lit(datetime.now()))
#df2.show(10)
hudi_options = {
    'hoodie.database.name': 'hudi_db',
    'hoodie.table.name': 'accounts_hudi',
    'hoodie.datasource.write.recordkey.field': 'account_id',
    'hoodie.datasource.write.table.name': 'accounts_hudi',
    'hoodie.datasource.write.operation': 'bulk_insert',
    'hoodie.datasource.write.precombine.field': 'ts'
}
df2.write.format('hudi').options(**hudi_options).mode('overwrite').save('s3://dbt-hudi-glue/Target/accounts_hudi')
job.commit()