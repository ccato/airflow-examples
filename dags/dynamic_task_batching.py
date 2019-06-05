from datetime import datetime, timedelta
from typing import List
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# The max parallel tasks limit
max_tasks = 5

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 5),
    'email': 'your.email@your.domain',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1,
    'max_active_runs': 1
}

dag = DAG(
    'dynamic_task_batching_v1', default_args=default_args, catchup=False,
    schedule_interval='10 0 * * *', max_active_runs=1)

# Your list of things to iterate goes here.
things_to_process = {
    'ThingIdentifyer1': 'ThingAction1',
    'ThingIdentifyer2': 'ThingAction2',
    'ThingIdentifyer3': 'ThingAction3',
    'ThingIdentifyer4': 'ThingAction4',
    'ThingIdentifyer5': 'ThingAction5',
    'ThingIdentifyer6': 'ThingAction6',
    'ThingIdentifyer7': 'ThingAction7',
    'ThingIdentifyer8': 'ThingAction8',
    'ThingIdentifyer9': 'ThingAction9',
    'ThingIdentifyer10': 'ThingAction10',
    'ThingIdentifyer11': 'ThingAction11',
    'ThingIdentifyer12': 'ThingAction12',
    'ThingIdentifyer13': 'ThingAction13',
}


def process_thing(**kwargs):
    thing_identifyer = kwargs['params']['thing_identifyer']
    thing_action = kwargs['params']['thing_action']
    print('Hey, I just processed {} that had the action {}'.format(thing_identifyer, thing_action))
    return True


t: List[PythonOperator] = []
c: int = 0
batchops = []
batchjoins = []
j: int = 0


for (thing_identifyer, thing_action) in things_to_process.items():

    # Start a new batch group to contain the tasks we run in parallel
    if j is 0:
        batchops.append(DummyOperator(
            task_id='batch_{}'.format(len(batchops)),
            dag=dag
        ))

        # if this is the first batch, we set up a dummy starting point before starting the batch
        if c is 0:
            startop = DummyOperator(
                task_id='Get_users_from_views',
                wait_for_downstream=True,
                dag=dag
            )
            batchops[0].set_upstream(startop)

        # if are in batch two and onwards, set the previous joiner as an upstream
        else:
            if len(batchops) > 0:
                batchops[-1].set_upstream(batchjoins[-1])

    # init the "real" worker that does the heavy lifting
    workerop = PythonOperator(
        task_id='dyn_task_{}'.format(thing_identifyer),
        python_callable=process_thing,
        dag=dag,
        params={
            'thing_identifyer': thing_identifyer,
            'thing_action': thing_action
        }
    )

    # set the batch group as the upstream
    workerop.set_upstream(batchops[-1])
    j += 1

    # finish batch group by joining the tasks in the group into a dummy joiner
    if j is max_tasks:
        batchjoins.append(DummyOperator(
            task_id='join_{}'.format(len(batchjoins)),
            dag=dag
        ))
        for task in batchops[-1].downstream_list:
            dag.get_task(task.task_id).set_downstream(batchjoins[-1])
        j = 0

    c += 1

