# Databricks notebook source
# MAGIC %md
# MAGIC ### View the latest COVID-19 hospitalization data
# MAGIC #### Setup 

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get and Transform data

# COMMAND ----------

data_path = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv'
print(f'Data path: {data_path}')

# COMMAND ----------

from covid_analysis.transforms import *
import pandas as pd

pdf = pd.read_csv(data_path)
pdf = filter_country(pdf, country='United States')
pdf = pivot_and_clean(pdf, fillna=0)  
pdf = clean_spark_cols(pdf)
pdf = index_to_col(pdf, colname='date')

# Convert from Pandas to a pyspark sql DataFrame.
df = spark.createDataFrame(pdf)

display(pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to Delta Lake
# MAGIC The current schema has spaces in the column names, which are incompatible with Delta Lake.  To save our data as a table, we'll replace the spaces with underscores.  We also need to add the date index as its own column or it won't be available to others who might query this table.
# MAGIC 
# MAGIC *Note: Please run the following cell to ensure that when you write a Delta table it won't conflict with anyone elses!*

# COMMAND ----------

dbutils.widgets.text("db_prefix", "covid_trends")
dbutils.widgets.text("reset_all_data", "false")

import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

db_prefix = dbutils.widgets.get("db_prefix")

dbName = db_prefix+"_"+current_user_no_at
cloud_storage_path = f"/Users/{current_user}/field_demos/{db_prefix}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")

print("using cloud_storage_path {}".format(cloud_storage_path))
print("new database created {}".format(dbName))

# COMMAND ----------

# Write to Delta Lake
df.write.mode('overwrite').saveAsTable('covid_stats')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize

# COMMAND ----------

# Using bamboolib output
import plotly.express as px
fig = px.line(df.sort_values(by=['date'], ascending=[True]).dropna(subset=['Daily_ICU_occupancy_per_million', 'Weekly_new_hospital_admissions_per_million', 'Daily_ICU_occupancy', 'Weekly_new_hospital_admissions']), x='date', y=['Daily_ICU_occupancy', 'Daily_ICU_occupancy_per_million', 'Daily_hospital_occupancy', 'Daily_hospital_occupancy_per_million', 'Weekly_new_hospital_admissions', 'Weekly_new_hospital_admissions_per_million'], template='plotly_white', title='Hospitalizations: 2020-2022')
fig.update_layout(legend_title_text='Indicator')
fig.update_xaxes(title_text='Date')
fig.update_yaxes(title_text='Patients')
fig

# COMMAND ----------

# Using Databricks visualizations and data profiling
display(spark.table('covid_stats'))
