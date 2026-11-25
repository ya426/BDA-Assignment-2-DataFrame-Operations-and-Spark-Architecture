import argparse
from pathlib import Path
from operator import add
from pyspark.sql import SparkSession, functions as F, types as T


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ui', action='store_true', help='Keep Spark UI open until Enter is pressed')
    parser.add_argument('--data', default=str(Path(__file__).parent / 'data' / 'sales.csv'))
    args = parser.parse_args()
    spark = (SparkSession.builder.appName('Retail Sales Business Intelligence')
             .master('local[4]').config('spark.sql.shuffle.partitions', '4').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        schema = T.StructType([T.StructField(n, T.StringType(), True) for n in
            ['transaction_id','product_name','category','store_location','quantity_sold','unit_price','transaction_date']])
        raw = spark.read.option('header', True).schema(schema).csv(args.data)
        df = (raw.withColumn('quantity_sold', F.expr('try_cast(quantity_sold as int)'))
              .withColumn('unit_price', F.expr('try_cast(unit_price as decimal(12,2))'))
              .withColumn('transaction_date', F.expr('try_cast(transaction_date as date)')))
        print('\n1. Dataset schema and structure'); df.printSchema(); df.show(100, truncate=False)
        print('\n2. Missing values per column (including failed type conversions)')
        df.select(*[F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast('string')) == ''), 1).otherwise(0)).alias(c) for c in df.columns]).show()
        invalid = F.lit(False)
        for c in df.columns:
            invalid = invalid | F.col(c).isNull() | (F.trim(F.col(c).cast('string')) == '')
        invalid = invalid | (F.col('quantity_sold') <= 0) | (F.col('unit_price') <= 0)
        print('\nInvalid records'); df.filter(invalid).show(truncate=False)
        clean = df.filter(~invalid)
        sales = clean.withColumn('total_sales', F.col('quantity_sold') * F.col('unit_price'))
        print('\n3. Transaction sales'); sales.orderBy('transaction_id').show(100, truncate=False)
        category = sales.groupBy('category').agg(F.sum('total_sales').alias('revenue')).orderBy(F.desc('revenue'), 'category')
        print('\n4. Category revenue'); category.show(truncate=False)
        print('\n5. Top five products by units sold')
        sales.groupBy('product_name').agg(F.sum('quantity_sold').alias('units_sold'), F.sum('total_sales').alias('revenue')).orderBy(F.desc('units_sold'), 'product_name').limit(5).show(truncate=False)
        print('\n6. Location revenue'); sales.groupBy('store_location').agg(F.sum('total_sales').alias('revenue')).orderBy(F.desc('revenue'), 'store_location').show(truncate=False)
        print('\n7. Transactions exceeding INR 10000'); sales.filter(F.col('total_sales') > 10000).orderBy(F.desc('total_sales'), 'transaction_id').show(100, truncate=False)
        print('\n8. Lazy evaluation and execution plan')
        pipeline = sales.filter(F.col('total_sales') > 10000).groupBy('category').agg(F.sum('total_sales').alias('revenue')).orderBy(F.desc('revenue'))
        print('Transformations defined. No action on this pipeline yet.')
        pipeline.explain(mode='extended')
        print('Calling show() to trigger execution'); pipeline.show()
        print('\n9. RDD versus DataFrame: same category aggregation')
        rdd = sales.select('category','total_sales').rdd.map(lambda r: (r.category, r.total_sales)).reduceByKey(add)
        print(rdd.toDebugString().decode('utf-8'))
        rdd_result = dict(rdd.collect())
        df_result = {r.category: r.revenue for r in category.collect()}
        print('RDD result:', sorted(rdd_result.items()))
        print('DataFrame result:', sorted(df_result.items()))
        assert rdd_result == df_result, 'Results differ'
        print('Results match:', True)
        print('Spark UI:', spark.sparkContext.uiWebUrl)
        if args.ui:
            input('Open Spark UI and capture Jobs, Stages, SQL and Executors. Press Enter to exit...')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
