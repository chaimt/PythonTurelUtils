import logging
import os

import yaml
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
import core.dynamic_imports as di

l_audit = di.import_relative_source(__file__, 'lib.audit')
l_gcs_hook = di.import_relative_source(__file__, 'lib.hooks.gcs_hook')
l_dic_utils = di.import_relative_source(__file__, 'lib.dic_utils')

CONN_ID_SLACK = 'slack'
CONN_ID_BQ_ZAZMA = 'bigquery_zazma'
CONN_ID_GCP_STORAGE = 'google_cloud_default'

ENV_PROD = "prod"
ENV_DEV = "dev"

SERVICE_REPORT = 'report'
SERVICE_MAINTENANCE = 'maintenance'
SERVICE_GLOBAL = 'global'
SERVICE_DATAFLOW = 'dataflow'
SERVICE_MONITOR = 'monitor'
SERVICE_BI_DWH = 'bi-dwh'
SERVICE_MIXPANEL = 'mixpanel'

SALESFORCE_CLASS = 'com.turel.migration.dataflow.salesforce.LoadSalesforceDataPipeline'
MONGODB_CLASS = 'com.turel.migration.dataflow.mongodb.LoadMongodbDataPipeline'

ACCOUNT_REPORT_TABLE = 'account_reports_scheduler'
AIRFLOW_DS = 'airflow'


# @not_in_use ...
# -------------------------------------------------------
def get_folder_index_from_path(file_name, folder):
    dag_path = di.get_path_folders(file_name)
    return dag_path.index(folder)


def load_file(file_name, stream):
    """
        Reads a stream associated with the file_name and saves it
        contents in the Cache. If the given file_name has yaml
        extension, it will be parsed and saved as an object (dict).

        This function is a wrapper for functions like load_config_file,
        load_config_file, etc.
    """
    prefix, suffix = os.path.splitext(file_name)
    if suffix.lower() in ['.yaml', '.yml']:
        return yaml.load(stream)
    else:
        return stream.decode()


def read_file(file_path):
    """
        Tries to download file located in file_path and save it in
        the Cache. If file is yaml-file it will be parsed and saved as
        an object, otherwise it will be saved as a plain text

        Returns (boolean):
            true if file was successfully uploaded and false otherwise
    """
    result = None
    file_name = file_path

    try:
        file_name = os.path.basename(file_path)
        if di.is_test_mode():
            l_audit.turel_log('reading file from local storage, file_location={0}'.format(file_path))

            with open(file_path) as stream:
                result = load_file(file_name, stream)
        else:
            gcp_hook = l_gcs_hook.GoogleCloudStorageHook(CONN_ID_GCP_STORAGE)

            file_location = file_path[file_path.find('/dags/') + 1:]
            l_audit.turel_log('action=download_file, file_location={0}'.format(file_location))

            if gcp_hook.exists(di.Settings.get_instance().g_bq_bucket, file_location):
                stream = gcp_hook.download(di.Settings.get_instance().g_bq_bucket, file_location)
                result = load_file(file_name, stream)
            else:
                l_audit.turel_log('action=download_file_missing, file_location={0}'.format(file_location))
    except Exception:
        l_audit.turel_log_exception('action=read_file, file={}'.format(file_name))

    return result


@di.CacheDecorator()
def get_resource_file(file_name, resource_name):
    result = None
    l_audit.turel_log('action=get_resource_file, file_name={0}, resource_name={1}'.format(file_name, resource_name))

    if di.is_test_mode():
        logging.warning('loading config from local storage')
        resource_path = 'dags/resources/' + resource_name
        resources_dir = os.path.abspath(os.path.join(os.path.dirname(file_name), '..', ''))
        with open(resources_dir + "/" + resource_path, 'rb') as stream:
            result = load_file(file_name, stream.read())

    else:
        gcp_hook = l_gcs_hook.GoogleCloudStorageHook(CONN_ID_GCP_STORAGE)
        project_file = di.get_project_from_path(file_name)
        env = di.get_env_from_path(file_name)

        file_location = 'dags/{env}/{project_file}/resources/{resource_file}'.format(
            env=env,
            project_file=project_file,
            resource_file=resource_name
        )

        l_audit.turel_log(
            'action=get_resource_file, bucket={0}, file_location={1}'.format(di.Settings.get_instance().g_bq_bucket, file_location))

        if gcp_hook.exists(di.Settings.get_instance().g_bq_bucket, file_location):
            stream = gcp_hook.download(di.Settings.get_instance().g_bq_bucket, file_location)
            result = load_file(file_name, stream)

    return result


# get contents of the given config file...
# -------------------------------------------------------
@di.CacheDecorator()
def get_config_file(file_name, resource_name, project_name=None):
    l_audit.turel_log(
        'action=get_config_file, file_name={0}, resource_name={1}, project_name={2}'.format(file_name, resource_name, project_name)
    )

    base_project = di.get_project_from_path(file_name)

    if base_project is None:
        base_project = project_name

    if di.is_test_env() and not di.is_test_mode():
        if project_name == di.C_COMMON_PROJECT_NAME:
            config_path = '/dags/{0}/{1}/lib/config'.format(di.Settings.get_instance().g_test_env, base_project)
        else:
            config_path = '/dags/{0}/{1}/config'.format(di.Settings.get_instance().g_test_env, base_project)
    else:
        if project_name == di.C_COMMON_PROJECT_NAME:
            config_path = di.get_project_config_path(__file__, di.C_COMMON_PROJECT_NAME)
        else:
            config_path = di.get_project_config_path(__file__, base_project)

    file = read_file(config_path + '/' + resource_name)
    if file is None and di.is_test_mode():
        config_path = di.get_project_root_path(__file__, base_project)
        file = read_file(config_path + '/test/config/' + resource_name)

    return file


#
# -------------------------------------------------------
def get_application_config(file_name, service=None, section=None):
    l_audit.turel_log('action=get_application_config, file_name={0}'.format(file_name))

    file_name = di.hybrid_check_file_name(file_name)
    env = di.get_env_from_path(file_name)

    try:
        # load common (cross-projects) config:
        application_base = get_config_file(file_name, 'application.yml', di.C_COMMON_PROJECT_NAME)
        try:
            application_env = get_config_file(file_name, 'application-{}.yml'.format(env), di.C_COMMON_PROJECT_NAME)
            if application_env is not None:
                l_dic_utils.dict_merge(application_base, application_env)
        except Exception as e:
            l_audit.turel_log_exception(
                'action=application_base, file={}, service={}, section={}, error={}'.format(
                    file_name, service, section, str(e)
                )
            )

        # load project-specific config:
        if service is not None:
            try:
                application_env = get_config_file(
                    file_name,
                    'application-{}.yml'.format(service),
                    di.get_project_from_path(file_name)
                )

                if application_env is not None:
                    l_dic_utils.dict_merge(application_base, application_env)
            except Exception as e:
                l_audit.turel_log_exception(
                    'action=application_env, file={}, service={}, section={}, error={}'.format(
                        file_name, service, section, str(e)
                    )
                )

            try:
                application_env = get_config_file(file_name, 'application-{}-{}.yml'.format(service, env), service)
                if application_env is not None:
                    l_dic_utils.dict_merge(application_base, application_env)
            except Exception as e:
                l_audit.turel_log_exception(
                    'action=application_env_a, file={}, service={}, section={}, error={}'.format(
                        file_name, service, section, str(e)
                    )
                )

            # load test file
            if di.is_test_mode():
                try:
                    application_env = get_config_file(
                        file_name,
                        'application-{}-test.yml'.format(service),
                        di.get_project_from_path(file_name)
                    )

                    if application_env is not None:
                        l_dic_utils.dict_merge(application_base, application_env)
                except Exception as e:
                    l_audit.turel_log_exception(
                        'action=application_env, file={}, service={}, section={}, error={}'.format(
                            file_name, service, section, str(e)
                        )
                    )

        # load versions:
        try:
            application_version = get_config_file(file_name, 'application-version.yml')
            if application_version is not None:
                l_dic_utils.dict_merge(application_base, application_version)
        except Exception as e:
            l_audit.turel_log_exception(
                'action=application_version, file={}, service={}, section={}, error={}'.format(
                    file_name, service, section, str(e)
                )
            )

        config = l_dic_utils.resolve_placeholders(application_base)
        if section is not None:
            return config[section]
        else:
            return config
    except yaml.YAMLError as exc:
        l_audit.turel_log_error(
            'action=global, file={}, service={}, section={}, error={}'.format(file_name, service, section, str(exc))
        )
    except Exception as e:
        l_audit.turel_log_exception(
            'action=global, file={}, service={}, section={}, error={}'.format(file_name, service, section, str(e))
        )

    return {'global': {}}


# get key from config dictionary, if params are not
# relevant returns None...
# -------------------------------------------------------
def get_config_value(dag_config, key, service=None):
    if type(dag_config) != dict:
        return None
    if service is None:
        if key in dag_config.keys():
            return dag_config[key]
        else:
            return None
    else:
        try:
            return dag_config[service][key]
        except (TypeError, KeyError):
            return None


# get global connection name...
# -------------------------------------------------------
def get_global_conn_id(dag_config, conn_name):
    result = ''
    if isinstance(dag_config, dict):
        result = l_dic_utils.get_value_by_path(dag_config, 'global.connections.{conn_name}'.format(conn_name=conn_name))

    return result


def slack_failed_task(context):
    failed_alert = SlackWebhookOperator(
        task_id='slack_failed_alert',
        http_conn_id='slack',
        username='airflow',
        webhook_token=BaseHook.get_connection(CONN_ID_SLACK).password,
        message="""
           :red_circle: Task Failed.
           *Task*: {task}
           *Dag*: {dag}
           *Execution Time*: {exec_date}
           *Log Url*: {log_url}
           """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
            # channel='#epm-marketing-dev',
            icon_emoji=':red_circle:',
            link_names=True
        ),
    )
    return failed_alert.execute


def task_slack_text(slack_msg, dag):
    return SlackWebhookOperator(
        task_id='slack_test_v1',
        http_conn_id='slack',
        webhook_token=BaseHook.get_connection(CONN_ID_SLACK).password,
        message=slack_msg,
        username='airflow',
        dag=dag
    )
