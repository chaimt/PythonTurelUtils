import sys
import logging
from datetime import timedelta, datetime

import croniter
import pytz
from airflow import DAG
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.hooks.base_hook import BaseHook
from airflow.utils import timezone
import hashlib

from core.dynamic_imports import import_relative_source, get_env_from_path, get_file_name, \
    is_test_mode, is_test_env, get_project_from_path, Settings

# Import and globals --------------------------------------------------------

l_config = import_relative_source(__file__, 'lib.config')
l_dic_utils = import_relative_source(__file__, 'lib.dic_utils')
l_audit = import_relative_source(__file__, 'lib.audit')
l_bigquery_hook = import_relative_source(__file__, 'lib.hooks.bigquery_hook')

# Constants  ----------------------------------------------------------------

C_SUCCESS_STATUS = "success"
C_FAILED_STATUS = "failed"


def check_dag_exec(dag_name):
    import sys
    logging.info("This is the name of the script: " + sys.argv[0])
    logging.info("The arguments are: " + str(sys.argv))
    if len(sys.argv) > 3 and sys.argv[1] == "test":
        run_dag = sys.argv[2]
        return run_dag == dag_name


def build_owner_list(owners):
    return ", ".join(owners)


def get_project_version(file_name, config=None, project_name=None):
    """
        Get project's version which should be stored in
        config/application-version.yml file

        Parameters:
            file_name (str): file where function was called
            config (dict): global project config dictionary; if empty,
            the config will be uploaded
            project_name (str): project name, if empty, it will be
            extracted from __file__

        Returns:
            (str): version number or empty string if version cannot be
            identified
    """
    if project_name is None:
        project_name = get_project_from_path(file_name)

    if config is None:
        config = l_config.get_application_config(file_name, project_name)

    project_version = ''
    try:
        project_version = config['version'][project_name]
    except Exception:
        l_audit.turel_log_exception('Error during extracting project version for [%s]' % project_name)

    l_audit.turel_log('[%s] version is: [%s]' % (project_name, project_version))
    return project_version.rstrip()


def get_env_suffix(env):
    if env == l_config.ENV_PROD:
        config_suffix = ''
    else:
        config_suffix = '_' + env

    return config_suffix


def get_start_date(cron):
    if cron is None or cron == '':
        return None
    if cron.startswith('@'):
        return cron

    now = timezone.utcnow()
    cron = croniter.croniter(cron, now)
    prev_date = cron.get_prev(datetime)
    next_date = cron.get_next(datetime)
    start_date = now - (next_date - prev_date) - timedelta(minutes=2)
    return start_date


def get_cron_interval(cron):
    now = timezone.utcnow()
    cron = croniter.croniter(cron, now)
    next_date = cron.get_next(datetime)
    seconds = (next_date - now).total_seconds()

    return seconds


def get_dag_id(file_name, version=None, prefix=None):
    # env / module / version
    if is_test_mode():
        env = get_env_from_path(file_name)
    else:
        if is_test_env():
            env = Settings.get_instance().g_test_env
        else:
            env = get_env_from_path(file_name)

    if version is None:
        version = ''
    if prefix is not None:
        prefix = '.' + prefix
    else:
        prefix = ''

    return "{0}.{1}{2}.{3}_{4}".format(
        env,
        get_project_from_path(file_name),
        prefix,
        get_file_name(file_name),
        version
    )


# generate dags name according to the given env where
# it should be executed, final dags name is:
#   <app_name>_<dag_name>[_<env>]
# ...
# ---------------------------------------------------
def get_dag_name(dag_name, project_name='', env='', version=''):
    if not project_name:
        project_name = get_project_from_path(__file__)

    result = project_name + '.' + dag_name
    if env is not None and env != '':
        result = env + '.' + result

    if version is not None and version != '':
        result = result + '-' + version

    return result

# get a parameter passed to dag using task_params in
# test command or conf in command trigger_dag (both -
# CLI)...
# ---------------------------------------------------
def get_dag_parameter(task_instance, dag_run, param_name):
    task = task_instance.task
    dag = task.dag

    result = l_dic_utils.get_dict_value(dag.params, param_name, '')
    if task is not None and param_name in task.params.keys():
        result = task.params[param_name]
    if dag_run is not None and dag_run.conf is not None and param_name in dag_run.conf.keys():
        result = dag_run.conf[param_name]

    return result


def fix_days_ago(n):
    now = timezone.utcnow()
    return days_ago(n, hour=now.hour, minute=now.minute)


def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    """
    today = timezone.utcnow().replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
    return today - timedelta(days=n)


def fix_minutes_ago(n):
    now = timezone.utcnow()
    return minutes_ago(n, hour=now.hour, minute=now.minute)


def minutes_ago(n, hour=0, minute=0, second=0, microsecond=0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    """
    today = timezone.utcnow().replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
    return today - timedelta(minutes=n)


def task_success_task(context):
    l_audit.turel_log('action=task_success_task, data=' + str(context))


def task_failure_task(context):
    l_audit.turel_log('action=task_failure_task, data=' + str(context))


class turelDAG(DAG):
    @staticmethod
    def get_external_trigger(kwargs):
        if kwargs['dag_run'] is None:
            external_trigger = False
        else:
            external_trigger = kwargs['dag_run']['external_trigger']

        return external_trigger

    @staticmethod
    def write_to_turel_log(info, msg_type=None):
        s = ''
        for key in info.keys():
            if s != '':
                s = s + ','
            s = s + key + '=' + str(info[key])

        if type(msg_type) != str:
            msg_type = ''
        else:
            msg_type = msg_type.lower()

        if msg_type == '' and 'status' in info.keys():
            msg_type = info['status'].lower()

        if msg_type == 'error':
            l_audit.turel_log_error(s)
        elif msg_type == 'warning':
            l_audit.turel_log_warning(s)
        elif msg_type == 'debug':
            l_audit.turel_log_debug(s)
        elif msg_type == 'exception':
            l_audit.turel_log_exception(s)
        else:
            l_audit.turel_log(s)

    @staticmethod
    def get_task_duration(context):
        try:
            duration = context['task_instance'].duration
            if duration is None:
                interval = datetime.now(pytz.utc) - context['ti'].start_date
                duration = interval.total_seconds()
        except KeyError:
            duration = None

        return duration

    @staticmethod
    def get_dag_run_id(context):
        result = ''

        try:
            if 'run_id' in context.keys() and context['run_id']:
                result = context['dag'].dag_id + '-r-' + context['run_id']

            if not result:
                result = context['dag'].dag_id + '-r-custom__' + context['dag'].start_date.strftime("%Y%m%d%H%M%S")

            if result:
                params = context['dag'].params
                if 'dag_run' in context.keys() and context['dag_run']:
                    params['dag_run_conf'] = context['dag_run'].conf
                result += '-h-' + hashlib.md5(str(params).encode()).hexdigest()
        except KeyError:
            result = None

        return result

    @staticmethod
    def __record_dag_status(context, status):
        ds = str(datetime.strptime(context.get('ds'), '%Y-%m-%d'))
        execution_date = context.get('execution_date')
        duration = turelDAG.get_task_duration(context)

        task_instance = context['task_instance']
        start_date = task_instance.start_date
        state = task_instance.state
        task_id = task_instance.task_id

        err_message = ''
        if status != C_SUCCESS_STATUS:
            try:
                err_message = context['exception'].args[0]
            except KeyError:
                err_message = ''

        try:
            version = context['dag'].params['version']
            if version == '':
                version = None
        except KeyError:
            version = None

        rows = [{
            "json": {
                "dag_id": context['dag'].dag_id,
                "dag_status": status,
                "dag_ds": ds,
                "dag_execution_date": str(execution_date),
                "task_id": task_id,
                "task_duration": duration,
                "task_start_date": str(start_date),
                "task_state": state,
                "err_message": err_message,
                'dag_version': version,
                'run_id': turelDAG.get_dag_run_id(context),
                'task_operator': task_instance.operator,
                'try_number': task_instance.try_number - 1
            }
        }]

        args = context['dag'].default_args
        conn_id = args['airflow_bq_conn_id']
        if conn_id is None:
            raise Exception('airflow_bq_conn_id is missing')

        hook = l_bigquery_hook.BigQueryHook(conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.insert_all(context.get('airflow_project_id'), args['airflow_dataset_id'], 'dag_status', rows)

    @staticmethod
    def __setup_params(dag_config, service, kwargs):
        service_config = l_dic_utils.get_dict_value(dag_config, service, {})

        if 'dag' in service_config and 'params' in service_config['dag']:
            params = service_config['dag']['params']
            # if 'dag_run' not in kwargs:
            #     kwargs['dag_run'] = DagRun(conf=params)
            for k, v in params.items():
                kwargs['params'][k] = v

    @staticmethod
    def __setup_version(dag_config, project_name, params):
        if params is None or not dag_config:
            return

        if 'version' not in params.keys() and 'version' in dag_config:
            params['version'] = dag_config['version'][project_name]

    def success_callback(self, context):
        # write status to database:
        self.__record_dag_status(context, C_SUCCESS_STATUS)

        # if there's custom listener call it:
        if self.__on_custom_success_callback:
            self.__on_custom_success_callback(context)

    def failure_callback(self, context):
        # write status to database:
        self.__record_dag_status(context, C_FAILED_STATUS)

        # in addition write status to slack channel:
        try:
            hook = SlackWebhookHook(
                http_conn_id=l_config.CONN_ID_SLACK,
                webhook_token=BaseHook.get_connection(l_config.CONN_ID_SLACK).password,
                message='dag_failure_task: ' + str(context),
                username='airflow',
            )
            hook.execute()
        except Exception:
            logging.error('Unexpected error during processing failure_callback: %s' % (sys.exc_info()[0]))

        # if there's custom listener call it:
        if self.__on_custom_failure_callback:
            self.__on_custom_failure_callback(context)

    def log_callback(self, info, event_type):
        if self.on_log_callback:
            info = self.on_log_callback(info, event_type, self)

        self.write_to_turel_log(info, event_type)

    def __save_custom_callback(self, callback, **kwargs):
        func = None

        if callback in kwargs:
            func = kwargs[callback]
        elif 'default_args' in kwargs and callback in kwargs['default_args']:
            func = kwargs['default_args'][callback]

        if callback == 'on_success_callback':
            self.__on_custom_success_callback = func
        elif callback == 'on_failure_callback':
            self.__on_custom_failure_callback = func

    def __init__(self, dag_config, filename, dag_id, *args, **kwargs):
        if kwargs is None:
            kwargs = {}

        service = get_file_name(filename)
        project_name = get_project_from_path(filename)

        # if description is passed as a parameter, use it,
        # otherwise try to take description from config:
        if 'description' not in kwargs:
            kwargs['description'] = l_config.get_config_value(dag_config, 'description', service)

        # use the same logic for schedule_interval:
        if 'schedule_interval' not in kwargs:
            kwargs['schedule_interval'] = l_config.get_config_value(dag_config, 'schedule_interval', service)

        # agolberg 05.06.2019 start_date may be passed as a
        # standard parameter:
        if 'start_date' not in kwargs:
            start_date_str = l_config.get_config_value(dag_config, 'start_date', service)
            if start_date_str is not None:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=None)
            else:
                start_date = get_start_date(kwargs['schedule_interval'])
                if start_date is None:
                    start_date = timezone.utcnow() - timedelta(days=2)  # Dvora:26/05-fix no schedule jobs

            kwargs['start_date'] = start_date

        # save custom "on log" event listener:
        self.on_log_callback = None
        if 'on_log_callback' in kwargs:
            self.on_log_callback = kwargs['on_log_callback']
            del kwargs['on_log_callback']

        # save custom "on failure" event listener:
        self.__save_custom_callback('on_failure_callback', **kwargs)
        # save custom "on success" event listener:
        self.__save_custom_callback('on_success_callback', **kwargs)

        # save project's version in dag's custom params:
        if 'params' not in kwargs:
            kwargs['params'] = {}

        self.__setup_params(dag_config, service, kwargs)
        self.__setup_version(dag_config, project_name, kwargs['params'])

        # @agolberg 04.07.2019 before these 2 rows had an opposite
        # order, but it's better to pass default_args to original
        # DAG constructor
        kwargs['default_args'] = self.build_dag_default_args(kwargs, dag_config, service, kwargs['start_date'])
        super(turelDAG, self).__init__(*args, **kwargs, dag_id=dag_id)

        self.dag_config = dag_config
        self.service = service

        # set up default events listeners:
        self.default_args['on_failure_callback'] = self.failure_callback
        self.default_args['on_success_callback'] = self.success_callback

        # save the fact of a dag creation in the common log:
        self.log_callback(
            {
                'action': 'create_dag',
                'dag_id': dag_id,
                'start_date': str(self.start_date),
                'schedule_interval': kwargs['schedule_interval']
            },
            'create_dag'
        )

    @staticmethod
    def add_dags_args(dict_info, dag_config):
        if not dag_config:
            return

        dict_info['bq_conn_id'] = l_dic_utils.get_value_by_path(
            dag_config,
            'global.connections.datalake_bigquery_conn_id'
        )

        dict_info['gcp_conn_id'] = l_dic_utils.get_value_by_path(
            dag_config,
            'global.connections.datalake_bigquery_conn_id'
        )

        dict_info['airflow_project_id'] = l_dic_utils.get_value_by_path(
            dag_config,
            'airflow.project_id'
        )

        dict_info['airflow_dataset_id'] = l_dic_utils.get_value_by_path(
            dag_config,
            'airflow.dataset_id'
        )

        dict_info['airflow_bq_conn_id'] = l_dic_utils.get_value_by_path(
            dag_config,
            'airflow.bq_conn_id'
        )

    @staticmethod
    def build_default_args(dag_config, add_config):
        turel_args = {}
        turel_args.update(add_config)
        turelDAG.add_dags_args(turel_args, dag_config)

        return turel_args

    @staticmethod
    def build_dag_default_args(kwargs, dag_config, service, start_date):
        service_config = l_dic_utils.get_dict_value(dag_config, service, {})
        if 'dag' in service_config:
            dag_args = service_config['dag']
        else:
            dag_args = None

        turel_args = {
            'email_on_failure': l_config.get_config_value(dag_config, 'email_on_failure', 'global'),
            'email': l_config.get_config_value(dag_args, 'email', 'global'),
            'email_on_retry': l_dic_utils.get_dict_value(dag_args, 'email_on_retry', False),
            'retries': l_dic_utils.get_dict_value(dag_args, 'retries', 2),
            'max_active_runs': l_dic_utils.get_dict_value(dag_args, 'max_active_runs', 1),
            'retry_delay': timedelta(seconds=60),
            'depends_on_past': l_dic_utils.get_dict_value(dag_args, 'depends_on_past', False),
            'start_date': start_date,
        }
        turel_args.update(kwargs.get('default_args', {}))

        if 'owner' not in turel_args and service in dag_config.keys():
            turel_args['owner'] = l_config.get_config_value(dag_config, 'owner', service)

        if 'params' in kwargs and 'version' in kwargs['params']:
            turel_args['owner'] = turel_args['owner'] + ", " + kwargs['params']['version']

        turelDAG.add_dags_args(turel_args, dag_config)

        return turel_args

    def task_op_kwargs(self, add_config):
        return self.build_default_args(self.dag_config, add_config)
