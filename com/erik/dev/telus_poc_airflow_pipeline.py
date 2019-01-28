from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=20)
}
dag = DAG(dag_id='run_telus_pipeline',
          default_args=default_args)

t1_bash = """gsutil -m mv gs://telus_poc_input/sample_geo_data* gs://telus_poc_ready"""
t2_bash = """
ts=$(date +%s)
run_job='gcloud dataflow jobs run telus_dataflow_$ts --gcs-location gs://telus_poc_ready/dataflow --format="value(id)"'
jobid=`eval $run_job`
echo "SUBMITTED DATAFLOW JOB: $jobid"
done=0
max=100
i=0
while : ; do
  if [[ $i -gt $max ]];then
        echo "Max wait exceeded for step, exiting..."
        exit -1
  fi
  echo "Checking status..."
  check_status='gcloud dataflow jobs show '$jobid' --format="value(state)"'
  status=`eval $check_status`
  echo "DATAFLOW JOB with id $jobid is $status"
  if [[ $status == 'Done' ]]; then
        echo "Dataflow job done ... moving on"
        break
  elif [[ $status == 'Failed' ]]; then
        echo "Job failed, exiting..."
        exit -1
  else
        sleep 5
  fi

  i=$i+1
done
"""
t3_bash = """gsutil -m mv gs://telus_poc_ready/sample_geo_data* gs://telus_poc_input/archived"""
t1 = BashOperator(
    task_id='copy_files',
    bash_command=t1_bash,
    dag=dag)
t2 = BashOperator(
    task_id='run_dataflow_template',
    bash_command=t2_bash,
    dag=dag
)
t3 = BashOperator(
    task_id='archive_files',
    bash_command=t3_bash,
    dag=dag)
t1>>t2>>t3
