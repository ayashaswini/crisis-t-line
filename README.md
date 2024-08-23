# follow requirement as below

Need to follow Medallion Architecture to
process and transform the MH-CLD-2021-DS0001 SAMHSA dataset using PySpark. 

1. Read the CSV / parquet file from the Volumes:
  b. Explore the dataset and understand the schema and data types.
  c. Implement schema checks to ensure the integrity of the input data.
2. Implement the Medallion Architecture :
  a. Staging (Bronze) Layer:
    i. Create a bronze table to store the raw data from the CSV file.
    ii. Perform basic data validation and cleaning, such as handling null values,
        data types, and column names.
    iii. Partition the bronze table by an appropriate column(s) to improve query
        performance.

b. Transformation (Silver) Layer:
  i. Create a silver table to store the transformed and enriched data.
  ii. Perform more complex transformations (See Transformation Exercise
  for Senior Data Engineer for details), such as data aggregations, and
  data quality checks.
  iii. Partition the silver table by an appropriate column(s) to improve query
  performance.
  iv. Implement schema checks to ensure the transformed data matches the
  expected schema.
  v. Prepare to discuss how you would handle monitoring to track the success
  and failure of the silver layer processing.

c. Business (Gold) Layer:
  i. Create a gold table to store the final, curated data.
  ii. Perform additional transformations and calculations to create
  business-ready datasets.
  iii. Partition the gold table by an appropriate column(s) to improve query
  performance.
  iv. Prepare to discuss how you would handle schema checks to ensure the
  final data meets the expected schema.
  v. Prepare to discuss how you would handle monitoring to track the success
  and failure of the gold layer processing.


Perform below transformations:
  1. Data Type Conversion
    a. Convert the categorical variables (GENDER, RACE, ETHNICITY, MARITAL,
    EMPLOY, INCOME) from numeric values to appropriate string or categorical data
    types.
    b. Ensure that the CASEID variable is stored as an integer data type.
    c. Convert the numeric variables (MENHLTH, PHYHLTH, POORHLTH) to float data
    types.
    d. Validate the data types for all variables to ensure they are consistent with the
    expected formats.
    e. Provide a summary of the data type conversions performed.
  2. Data Normalization and Standardization
    a. Normalize the numeric variables (MENHLTH, PHYHLTH, POORHLTH) using
    min-max scaling to bring them to a common scale between 0 and 1.
    b. Standardize the numeric variables (MENHLTH, PHYHLTH, POORHLTH) using
    z-score normalization to have a mean of 0 and a standard deviation of 1.
    c. Ensure that the normalized and standardized variables are stored in new
    columns, keeping the original variables intact.
    d. Provide a summary of the normalization and standardization techniques used
    and the rationale behind them.
  3. Data Partitioning and Sampling
    a. Split the dataset into training and testing sets, ensuring a representative split
    based on the demographic variables (GENDER, RACE, ETHNICITY, MARITAL,
    EMPLOY, INCOME).
    b. Implement stratified sampling to create the training and testing sets, maintaining
    the same proportions of the demographic variables in both sets.
    c. Create a validation set from the training set using a similar stratified sampling
    approach.
    d. Ensure that the dataset splits are stored in separate files or data structures, with
    appropriate naming conventions and metadata.
    e. Provide a detailed report on the data partitioning and sampling process, including
    the rationale for the chosen techniques and the characteristics of the resulting
    datasets.
  
