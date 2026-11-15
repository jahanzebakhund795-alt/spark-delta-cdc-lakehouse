import argparse
from pathlib import Path

def run(input_path,root,batch_id):
    from pyspark.sql import SparkSession,functions as F,Window
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
    builder=(SparkSession.builder.appName('cdc-lakehouse').master('local[*]')
             .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
             .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark=configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        raw=spark.read.schema('order_id string, version long, op string, region string, amount_cents long').json(input_path)
        bronze=raw.withColumn('batch_id',F.lit(batch_id)).withColumn('ingested_at',F.current_timestamp())
        bronze.write.format('delta').mode('append').save(str(root/'bronze'))
        valid=(F.col('order_id').isNotNull() & (F.length('order_id')>0) &
               (F.col('version')>0) & F.col('op').isin('upsert','delete') &
               ((F.col('op')=='delete') | (F.col('amount_cents')>=0)))
        bronze.filter(~F.coalesce(valid,F.lit(False))).write.format('delta').mode('append').save(str(root/'quarantine'))
        clean=(raw.filter(valid).fillna({'region':'UNKNOWN'})
               .withColumn('is_deleted',F.col('op')=='delete')
               .withColumn('amount_cents',F.when(F.col('is_deleted'),F.lit(0)).otherwise(F.col('amount_cents'))))
        columns=['order_id','version','op','region','amount_cents','is_deleted']
        clean=clean.select(*columns).withColumn('fingerprint',F.sha2(F.to_json(F.struct(*columns)),256))
        conflicts=(clean.groupBy('order_id','version').agg(F.countDistinct('fingerprint').alias('variants'))
                   .filter('variants > 1'))
        if conflicts.limit(1).count():
            raise ValueError('Conflicting versions in source batch; bronze retained for investigation')
        window=Window.partitionBy('order_id').orderBy(F.desc('version'))
        updates=clean.withColumn('rank',F.row_number().over(window)).filter('rank=1').drop('rank')
        silver=str(root/'silver')
        if not DeltaTable.isDeltaTable(spark,silver):
            updates.limit(0).write.format('delta').save(silver)
        target=DeltaTable.forPath(spark,silver)
        clashes=updates.alias('s').join(target.toDF().alias('t'),'order_id').filter(
            (F.col('s.version')==F.col('t.version')) & (F.col('s.fingerprint')!=F.col('t.fingerprint')))
        if clashes.limit(1).count():
            raise ValueError('Conflicting version already present in silver')
        (target.alias('t').merge(updates.alias('s'),'t.order_id=s.order_id')
         .whenMatchedUpdateAll(condition='s.version>t.version')
         .whenNotMatchedInsertAll().execute())
        gold=target.toDF().filter('NOT is_deleted').groupBy('region').agg(
            F.sum('amount_cents').alias('revenue_cents'),F.count('*').alias('orders'))
        gold.write.format('delta').mode('overwrite').save(str(root/'gold'))
        return {'silver_rows':target.toDF().count(),'active_orders':target.toDF().filter('NOT is_deleted').count()}
    finally:
        spark.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--input',required=True)
    parser.add_argument('--root',type=Path,default=Path('output/lakehouse'))
    parser.add_argument('--batch-id',required=True)
    args=parser.parse_args()
    print(run(args.input,args.root,args.batch_id))
