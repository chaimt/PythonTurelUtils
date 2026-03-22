import re
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import ShortCircuitOperator
from core.dynamic_imports import allow_dag, get_file_name, import_relative_source

if allow_dag(__file__):
    l_config = import_relative_source(__file__, "lib.config")
    l_audit = import_relative_source(__file__, "lib.audit")
    l_dag_utils = import_relative_source(__file__, "lib.dag_utils")
    gcp_dataflow_hook = import_relative_source(__file__, "lib.hooks.gcp_dataflow_hook")

    def check_running_jobs(**kwargs):
        l_audit.company_log("check_running_jobs")
        gcp_conn_id = kwargs["gcp_conn_id"]
        options = kwargs["options"]
        patterns = options["patterns"]
        search_pattern = {}
        for pattern in patterns:
            search_pattern[pattern["name_pattern"]] = pattern["time_limit"]

        processes_to_kill = []
        hook = gcp_dataflow_hook.DataFlowHook(gcp_conn_id=gcp_conn_id, delegate_to=None, poll_sleep=10)
        jobs = hook.get_all_running_jobs(options)
        if jobs is not None:
            for job in jobs:
                name = job["name"]
                for pattern in patterns:
                    full_regex_match = re.search(pattern["name_pattern"], name)
                    if full_regex_match is not None:
                        start_time = datetime.strptime(job["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None)
                        if start_time + timedelta(minutes=pattern["time_limit"]) < datetime.now():
                            process = {"name": name, "job_id": job["id"]}
                            processes_to_kill.append(process)
                            l_audit.company_log("job " + str(job))

        l_audit.company_log("")
        l_audit.company_log("Processes Marked to Cancel: ")
        if processes_to_kill and len(processes_to_kill) > 0:
            for process in processes_to_kill:
                l_audit.company_log(str(process))
            kwargs["ti"].xcom_push(key="check_running_jobs.processes_to_kill", value=processes_to_kill)
            return True
        else:
            l_audit.company_log("No Processes Marked to Cancel Found")
            return False

    def cancel_long_jobs(**context):
        l_audit.company_log("check_running_jobs")
        processes_to_kill = context["ti"].xcom_pull(task_ids=check_running_jobs_op.task_id, key="check_running_jobs.processes_to_kill")
        if processes_to_kill:
            options = context["options"]
            for process in processes_to_kill:
                bash_command = "gcloud dataflow jobs --project={} cancel --region={} {}".format(
                    options["project"], options["region"], process["job_id"]
                )
                l_audit.company_log("bash_command: " + bash_command)
                bash = BashOperator(task_id="cancel_job", bash_command=bash_command, dag=dag)
                bash.execute(context)

            return len(processes_to_kill) > 0
        return False

    service = get_file_name(__file__)
    dag_config = l_config.get_application_config(__file__, l_config.SERVICE_MAINTENANCE)
    service_config = dag_config[get_file_name(__file__)]
    DAG_ID = l_dag_utils.get_dag_id(__file__, "v1")
    with l_dag_utils.MyDAG(dag_config, __file__, DAG_ID, max_active_runs=1) as dag:
        check_running_jobs_op = ShortCircuitOperator(
            task_id="check_running_jobs",
            provide_context=True,
            python_callable=check_running_jobs,
            op_kwargs={
                "gcp_conn_id": service_config["gcp_conn_id"],
                "options": service_config["options"],
                "slack_conn_id": l_config.get_global_conn_id(dag_config, "system_slack_conn_id"),
            },
            dag=dag,
        )

        cancel_long_jobs_op = ShortCircuitOperator(
            task_id="cancel_long_jobs",
            provide_context=True,
            python_callable=cancel_long_jobs,
            op_kwargs={
                "gcp_conn_id": service_config["gcp_conn_id"],
                "options": service_config["options"],
                "slack_conn_id": l_config.get_global_conn_id(dag_config, "system_slack_conn_id"),
            },
            dag=dag,
        )

        send_processes_killed_email_op = EmailOperator(
            task_id="send_processes_killed_email",
            to=["chaim@gmail.com"],
            subject="stopping jobs",
            html_content="""
            <html>
                <body>
                    <h2>Dataflow job cancellation</h2>
                    <ul>
                    {% for process_killed in task_instance.xcom_pull(task_ids='kill_halted_tasks', key='kill_halted_tasks.processes_to_kill') %}
                        <li>Process {{loop.index}}</li>
                        <ul>
                        {% for key, value in process_killed.items() %}
                            <li>{{ key }}: {{ value }}</li>
                        {% endfor %}
                        </ul>
                    {% endfor %}
                    </ul>
                </body>
            </html>
            """,
            dag=dag,
        )

        check_running_jobs_op.set_downstream(cancel_long_jobs_op)
        cancel_long_jobs_op.set_downstream(send_processes_killed_email_op)
