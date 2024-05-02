import findspark
findspark.init()

# import thư viện
import pandas as pd
import pyspark

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, lit, greatest, concat_ws, first
from pyspark.sql import functions as F
import time
import datetime

from datetime import datetime, timedelta

# mysql
user_my_sql = 'root'
password_my_sql = ''
serverName_my_sql = 'localhost'
database_my_sql = 'etl_log_data'
driver_my_sql = "com.mysql.cj.jdbc.Driver"
table_my_sql = 'summary_behavior_data'

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("ChangeColumnType") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.executor.memory", "15g") \
    .config("spark.executor.cores", "8") \
    .config("spark.driver.memory", "15g") \
    .config("spark.driver.cores", "8") \
    .getOrCreate()


def load_data(path):
    df = spark.read.json(path)

    df = df.select('_source.*')

    return df


def process_data(df, path):
    from pyspark.sql.functions import to_date, lit

    print('Insert Type Column')

    df = df.withColumn(
        "Type",
        when(
            (col("AppName") == 'CHANNEL') |
            (col("AppName") == 'DSHD') |
            (col("AppName") == 'KPLUS') |
            (col("AppName") == 'KPlus'),
            "TV"
        )
        .when(
            (col("AppName") == 'VOD') |
            (col("AppName") == 'FIMS_RES') |
            (col("AppName") == 'BHD_RES') |
            (col("AppName") == 'VOD_RES') |
            (col("AppName") == 'FIMS') |
            (col("AppName") == 'BHD') |
            (col("AppName") == 'DANET'),
            "Movie"
        )
        .when(
            (col("AppName") == 'RELAX'),
            "Relax"
        )
        .when(
            (col("AppName") == 'CHILD'),
            "Child"
        )
        .when(
            (col("AppName") == 'SPORT'),
            "Sport")
        .otherwise("Others")
    )

    print('Select Contract, TotalDuration, Type Column')

    df = df.select('Contract', 'TotalDuration', 'Type')

    print('Get Date String From File Path')
    date_str = path.split('\\')[-1].split('.')[0]

    print('Create Date Column')
    df = df.withColumn('Date', to_date(lit(date_str), 'yyyyMMdd'))

    return df


def pivot_data(df):
    df = df.groupBy('Contract', 'Type', 'Date').agg(
        F.sum('TotalDuration').alias('TotalDuration')
    )

    df = df.groupBy('Contract', 'Date').pivot('Type').agg(
        F.sum('TotalDuration').alias('TotalDuration')
    )

    df = df.fillna(0)

    return df


def most_watch(df):
    # Lấy ra các giá trị max theo từng dòng
    max_value = greatest(col('TV'), col('Movie'), col('Relax'), col('Child'), col('Sport'))

    df = df.withColumn(
        'Most Watch',
        when(col('TV') == max_value, 'TV')
        .when(col('Movie') == max_value, 'Movie')
        .when(col('Relax') == max_value, 'Relax')
        .when(col('Child') == max_value, 'Child')
        .when(col('Sport') == max_value, 'Sport')
    )

    return df


def customer_taste(df):
    cust_taste = concat_ws(
        ' - ',
        when(col('TV') != 0, 'TV'),
        when(col('Movie') != 0, 'Movie'),
        when(col('Relax') != 0, 'Relax'),
        when(col('Child') != 0, 'Child'),
        when(col('Sport') != 0, 'Sport')
    )

    df = df.withColumn(
        'Customer Taste', cust_taste
    )

    df = df.withColumn(
        'Customer Taste',
        when(col('Customer Taste') == 'TV - Movie - Relax - Child - Sport', 'All')
        .otherwise(col('Customer Taste'))
    )

    return df


def customer_activeness(df, from_date, to_date):
    from_date = datetime.strptime(from_date, '%Y%m%d').date()
    to_date = datetime.strptime(to_date, '%Y%m%d').date()

    total_duration = col('TV') + col('Movie') + col('Relax') + col('Child') + col('Sport')

    total_days = (to_date - from_date).days + 1

    df_temp = df.select(*[col(col_name) for col_name in df.columns])

    df_temp = df_temp.withColumn(
        'Customer Activeness', total_duration
    )

    df_temp = df_temp.withColumn(
        'Customer Activeness',
        when(col('Customer Activeness') > 0, 1)
        .otherwise(0)
    )

    df_temp = df_temp.withColumn(
        'Total Days', lit(total_days)
    )

    df_temp = df_temp.groupBy('Contract').agg(
        F.sum('Customer Activeness').alias('Customer Activeness')
    )

    df_temp = df_temp.withColumn(
        'Customer Activeness',
        F.round(col('Customer Activeness') / total_days, 2)
    )

    df_temp = df_temp.withColumn(
        'Customer Activeness',
        when(col('Customer Activeness') <= 0.4, 'Low')
        .otherwise('High')
    )

    df = df.join(df_temp, on=['Contract'])

    return df


def etl_1_day(path):
    print(f'Reading data from source {path}')
    df = load_data(path)

    print('Processing data')
    df = process_data(df, path)

    print('Pivot data')
    df = pivot_data(df)

    return df


def create_date_list(from_date, to_date):
    from_date = datetime.strptime(from_date, '%Y%m%d').date()
    to_date = datetime.strptime(to_date, '%Y%m%d').date()

    date_list = list()

    current_date = from_date
    while current_date <= to_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)

    date_list = list(map(lambda x: x.strftime('%Y%m%d'), date_list))

    return date_list


def write_to_mysql(df, user_my_sql, password_my_sql, serverName_my_sql, database_my_sql, driver_my_sql, table_my_sql):
    url_my_sql = f"jdbc:mysql://{serverName_my_sql}:3306/{database_my_sql}"

    connection_properties = {
        "user": user_my_sql,
        "password": password_my_sql,
        "driver": driver_my_sql
    }

    # ETL_DATA
    df.write.jdbc(url=url_my_sql, table=table_my_sql, mode='overwrite', properties=connection_properties)

    print('Successfully Write to MySQL!')


def main_task(source_folder, save_folder, from_date, to_date):
    import glob
    import datetime
    from datetime import datetime, timedelta
    import time

    start_time = time.time()

    # create date list from date and to date
    print(f'Create Date List From {from_date} and {to_date}')
    date_list = create_date_list(from_date, to_date)

    # lấy ra all các file json trong folder
    print(f'Create File List in Source Folder')
    file_list = glob.glob(rf'{source_folder}\*.json')

    k = 0

    print('ETL All Files And Put It To 1 Data Frame')
    for file_path in file_list:
        file_name = file_path.split('\\')[-1].split('.')[0]

        # if file_name in date_list => continue
        if file_name in date_list:
            if k == 0:
                df = etl_1_day(file_path)
            else:
                df = df.unionByName(etl_1_day(file_path), allowMissingColumns=True)

            k += 1

    print('Create Customer Activeness Column')
    df = customer_activeness(df, from_date, to_date)

    print('Group By All Date')
    df = df.groupBy('Contract').agg(
        F.sum('Child').alias('Child'),
        F.sum('Movie').alias('Movie'),
        F.sum('Relax').alias('Relax'),
        F.sum('Sport').alias('Sport'),
        F.sum('TV').alias('TV'),
        F.first('Customer Activeness').alias('Customer Activeness')
    )

    print('Create Most Watch Column')
    df = most_watch(df)

    print('Create Custom Taste Column')
    df = customer_taste(df)

    df.show()

    print(f'Saving Final Result to {save_folder}')
    df.repartition(1).write.csv(rf'{save_folder}', header=True, mode='overwrite')

    print('Write Data to MySQL')
    write_to_mysql(df, user_my_sql, password_my_sql, serverName_my_sql, database_my_sql, driver_my_sql, table_my_sql)

    finish_time = time.time()

    print(f'Total Duration: {round(finish_time - start_time, 2)} seconds')


from_date = '20220401'
to_date = '20220430'
source_folder = r'G:\DE6\DATA\Dataset\log_content'
save_folder = rf'G:\DE6\Class 3 - Basic ETL\Result\Method1\{from_date}-{to_date}'

# 247.17 seconds
main_task(source_folder, save_folder, from_date, to_date)