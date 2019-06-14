# Databricks notebook source
dbutils.widgets.text("sql", "", "Enter SQL query")
dbutils.widgets.text("dry_run", "false", "dry run")

# COMMAND ----------

dry_run = dbutils.widgets.get("dry_run")
print(dry_run)
if dry_run == "true": 
  dbutils.notebook.exit("dry,run")
else:
  import io, csv
  sql = dbutils.widgets.get("sql")
  rows = spark.sql(sql).collect()
  si = io.StringIO()
  cw = csv.writer(si)
  cw.writerow(rows[0].asDict().keys())
  for row in rows:
    cw.writerow(row)

  dbutils.notebook.exit(si.getvalue())

# COMMAND ----------

