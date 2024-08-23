def main():
    """Main ETL script definition.
    :return: None
    """
    try:
        # start Spark application and get Spark session, logger and config
        spark, log, config = start_spark(
            app_name='my_etl_job')
    
        # log that main ETL job is starting
        log.warn('etl_job is up-and-running')
    
        # execute ETL pipeline
        input_data = extract_data(spark)
        # null check
        not_null_df = input_data.filter("_col is not NULL")
        store_in_ext_table(not_null_df,"bronze_table","part_fields") #store raw data in broze layer
        data_transformed = basic_transform_data(not_null_df) 
        store_in_ext_table(data_transformed,"silver_table","part_fields") #store data in silver layer
        final_df = final_transform(data_transformed)
        store_in_ext_table(final_df,"gold_table","part_fields") #store data in gold layer
        # log the success and terminate Spark application
        log.warn('test_etl_job is finished')
      return None
    except Exception as e:
        log.error(e.message())

def extract_data(spark):
    try:
       return spark.read.format("csv")
          .option("header", "true")
          .load("/databricks-datasets/MH-CLD-2021-DS0001-bndl-data-csv_v1.csv")
    except Exception as e:
        log.error(e.message())

def store_in_ext_table(data, table_name,part_fields):
    try:
        data.write.mode("append").format("delta").option("overwriteSchema","true").partitionBy(part_fields).saveAsTable(table_name)
    except Exception as e:
        log.error(e.message())

def basic_transform_data(input_df):

    try:
      #convert GENDER, RACE, ETHNICITY, MARITAL,EMPLOY, INCOME) from numeric values to appropriate string or categorical data
      # Ensure that the CASEID variable is stored as an integer data type
      # Convert the numeric variables (MENHLTH, PHYHLTH, POORHLTH) to float data types
      return input_df.withColumn("GENDER",col("GENDER").cast(StringType))
              .withColumn("RACE",col("RACE").cast(StringType))
              .withColumn("ETHNICITY",col("ETHNICITY").cast(StringType))
              .withColumn("MARITAL",col("MARITAL").cast(StringType))
              .withColumn("EMPLOY",col("EMPLOY").cast(StringType))
              .withColumn("INCOME",col("INCOME").cast(StringType))
              .withColumn("CASEID",col("CASEID").cast(IntegerType))
              .withColumn("(MENHLTH,",col("(MENHLTH,").cast(FloatType))
              .withColumn("PHYHLTH,",col("PHYHLTH,").cast(FloatType))
              .withColumn("POORHLTH)",col("POORHLTH)").cast(FloatType))
    except Exception as e:
        log.error(e.message())

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.linalg import Vectors

def final_transform(df):
    try:
      #Standardize the numeric variables (MENHLTH, PHYHLTH, POORHLTH) using z-score normalization to have a mean of 0 and a standard deviation of 1.
      
      w = Window.partitionBy()
      for i in ["MENHLTH","PHYHLTH","POORHLTH"]:
        df_stand = df.withColumn("standard_"+i, (F.col((i) - F.mean((i).over(w)) / F.stddev(i).over(w))))
                              
      #Normalize the numeric variables (MENHLTH, PHYHLTH, POORHLTH) using min-max scaling to bring them to a common scale between 0 and 1.
        
      for i in ["MENHLTH","PHYHLTH","POORHLTH"]:
        # VectorAssembler Transformation - Converting column to vector type
        assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
    
        # MinMaxScaler Transformation
        scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
    
        # Pipeline of VectorAssembler and MinMaxScaler
        pipeline = Pipeline(stages=[assembler, scaler])
    
        # Fitting pipeline on dataframe
        df_norm = pipeline.fit(df_stand).transform(df_stand).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
      return df_norm
    except Exception as e:
        log.error(e.message())




