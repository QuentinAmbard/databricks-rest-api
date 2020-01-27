# Databricks notebook source
#Test to start a job with a custom sql request and get the result :

import requests, time
def run_sql_requests(request):
  r = requests.post('https://demo.cloud.databricks.com/api/2.0/jobs/runs/submit', headers={"Authorization": "Bearer dapia68a0389cb4765c8a5ab4f4ebd248445"}, json={
    "run_name": "test rest api",
    "existing_cluster_id": "0614-080816-frier4",
    "notebook_task": {
      "notebook_path": "/Users/quentin.ambard@databricks.com/demo/Rest API demo",
      "base_parameters": {"sql": request}
    }
  })
  
  run_id = r.json()["run_id"]
  print(get_results(run_id))

def get_results(run_id):
  response = requests.get('https://demo.cloud.databricks.com/api/2.0/jobs/runs/get-output?run_id='+str(run_id), headers={"Authorization": "Bearer dapia68a0389cb4765c8a5ab4f4ebd248445"})
  r = response.json()
  if "metadata" in r and "state" in r["metadata"] and "result_state" in r["metadata"]["state"] and r["metadata"]["state"]["result_state"] == "FAILED":
    return "JOB FAILED, check sql request:"+str(response.content)
  if "error" in r or "result" not in r["notebook_output"]:
    time.sleep(1)
    return get_results(run_id)
  else:
    return r["notebook_output"]["result"]




# COMMAND ----------

result = run_sql_requests("""select * from quentin.bike_toulouse where name like "%OZE%" """)
print(result)

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.getContext.notebookPath

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from quentin.bike_toulouse where name like "%OZE%"

# COMMAND ----------

