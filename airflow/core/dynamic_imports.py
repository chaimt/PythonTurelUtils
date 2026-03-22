import importlib
import importlib.util as import_util
import logging
import os
import sys
from datetime import timedelta

from airflow.models import Variable
from airflow.utils import timezone


class CacheDecorator(object):

    def __init__(self, filename_param_pos=0):
        self.filename_param_pos = filename_param_pos

    def __call__(self, fn, *args, **kwargs):
        def wrapper(*args, **kwargs):
            project_file = get_project_from_path(args[self.filename_param_pos])
            env = get_env_from_path(args[self.filename_param_pos])

            cash_variables = Cache.get_variables()
            memoization_key = env + "_" + project_file + "_" + str(self._convert_call_arguments_to_hash(args, kwargs))
            if memoization_key not in cash_variables:
                cash_variables[memoization_key] = fn(*args, **kwargs)
            return cash_variables[memoization_key]

        return wrapper

    @staticmethod
    def _convert_call_arguments_to_hash(args, kwargs):
        return hash(str(args) + str(kwargs))


class Cache:
    __instance = None

    @staticmethod
    def get_instance():
        if Cache.__instance is None:
            Cache()

        return Cache.__instance

    def __init__(self):
        if Cache.__instance is not None:
            raise Exception("Cache is a singleton!")
        else:
            self.__cache_variables = dict()
            self.__cache_valid = timezone.utcnow()

            Cache.__instance = self

    def clear(self):
        __cache_variables = dict()
        __cache_valid = timezone.utcnow()

    @staticmethod
    def get_variables():
        cache = Cache.get_instance()
        if cache.__cache_valid + timedelta(minutes=5) < timezone.utcnow():
            cache.clear()

        return cache.__cache_variables

    @staticmethod
    def get_variable(name, default_value=None):
        cache_variables = Cache.get_instance().get_variables()

        if name in cache_variables:
            return cache_variables[name]
        else:
            try:
                value = Variable.get(name)
            except KeyError:
                value = default_value

            cache_variables[name] = value
            return value


class Settings:
    __instance = None

    @staticmethod
    def get_instance():
        if Settings.__instance is None:
            Settings()

        return Settings.__instance

    def __init__(self):
        if Settings.__instance is not None:
            raise Exception("Settings is a singleton!")
        else:
            self.g_bq_bucket = Cache.get_variable("BQ_JOBS_BUCKET")
            self.g_test_mode = Cache.get_variable("TEST_MODE", None)
            self.g_test_env = Cache.get_variable("TEST_ENV", None)
            self.g_test_dag = Cache.get_variable("TEST_DAG", None)

            Settings.__instance = self

    def set_ut_params(self, bq_bucket, test_mode, test_env, test_dag):
        self.g_bq_bucket = bq_bucket
        self.g_bq_bucket = test_mode
        self.g_test_env = test_env
        self.g_test_dag = test_dag


C_COMMON_PROJECT_NAME = "bi-airflow-common"


def is_hybrid_mode():
    return "hybrid" == Settings.get_instance().g_test_mode


def is_test_mode():
    return "True" == Settings.get_instance().g_test_mode or "hybrid" == Settings.get_instance().g_test_mode


def is_test_env():
    return Settings.get_instance().g_test_env is not None


def get_file_name(file_name):
    return os.path.basename(file_name).replace(".pyc", "").replace(".py", "")


def allow_dag(file_name):
    if is_test_mode():
        dag_name = get_file_name(file_name)
        if Settings.get_instance().g_test_dag is not None:
            return dag_name in Settings.get_instance().g_test_dag.split(",")
    return True


# split path to list of folders...
# -------------------------------------------------------
def get_path_folders(path):
    folders = []
    while 1:
        path, folder = os.path.split(path)

        if folder != "":
            folders.append(folder)
        else:
            if path != "":
                folders.append(path)
            break

    folders.reverse()
    return folders


def hybrid_check_file_name(file_name):
    if "hybrid" == Settings.get_instance().g_test_mode:
        env = get_env_from_path(file_name)
        dag_path = get_path_folders(file_name)
        dag_index = dag_path.index("dags")
        project = dag_path[dag_index - 1]
        dag_index = file_name.index("dags") + len("dags")
        suffix = file_name[dag_index + 1 :]
        return "/home/airflow/gcs/dags/{0}/{1}/{2}".format(env, project, suffix)
    else:
        return file_name


# get environment by file's path...
# -------------------------------------------------------
def get_env_from_path(file_name):
    logging.info("[turel]: get_env_from_path {%s}" % file_name)

    # there's no env on local machines:
    if is_test_env():
        result = Settings.get_instance().g_test_env
    # server folders structure: ..dags/env/project/..:
    else:
        dag_path = get_path_folders(file_name)
        dag_index = dag_path.index("dags")
        result = dag_path[dag_index + 1]

    return result


# get project by file's path...
# -------------------------------------------------------
def get_project_from_path(file_name):
    # local folders structure: ../project/dags/..:
    if (is_test_mode() or is_test_env()) and not is_hybrid_mode():
        dag_path = get_path_folders(file_name)
        dag_index = dag_path.index("dags")
        result = dag_path[dag_index - 1]
    # server folders structure: ..dags/env/project/..:
    else:
        dag_path = get_path_folders(file_name)
        dag_index = dag_path.index("dags")
        result = dag_path[dag_index + 2]

    return result


# get common project's root folder by file's path...
# -------------------------------------------------------
def get_common_root_path(file_name):
    dag_path = get_path_folders(file_name)
    dag_index = dag_path.index("dags")

    # local folders structure: ../project/dags/...
    if is_test_mode() or is_test_env():
        result = "/" + "/".join(dag_path[1 : (dag_index - 1)]) + "/" + C_COMMON_PROJECT_NAME
    # server folders structure: ../dags/env/project/...
    else:
        result = "/" + "/".join(dag_path[1 : (dag_index + 3)])

    return result


# get common project's dags folder by file's path...
# -------------------------------------------------------
def get_common_dag_path(file_name):
    result = get_common_root_path(file_name)

    # local folders structure: ../project/dags/..
    # server folders structure: ../dags/env/project/..
    if is_test_mode() or is_test_env():
        result = result + "/dags"

    return result


# get common project's root folder by file's path...
# -------------------------------------------------------
def get_project_root_path(file_name, project_name=None):
    if project_name == C_COMMON_PROJECT_NAME:
        result = get_common_root_path(file_name)
    else:
        if project_name is None:
            project_name = get_project_from_path(file_name)

        # local folders structure: ../project/dags/...
        if is_test_mode():
            dag_path = get_path_folders(file_name)
            dag_index = dag_path.index("dags")
            result = "/" + "/".join(dag_path[1 : (dag_index - 1)]) + "/" + project_name
        # server folders structure: ../dags/env/project/...
        else:
            dag_path = get_path_folders(file_name)
            dag_index = dag_path.index("dags")
            result = "/" + "/".join(dag_path[1 : (dag_index + 2)]) + "/" + project_name

    return result


# get project's dags folder by file's path...
# -------------------------------------------------------
def get_project_dag_path(file_name):
    result = get_project_root_path(file_name)

    # local folders structure: ../project/dags/..
    # server folders structure: ../dags/env/project/..
    if is_test_mode():
        result = result + "/dags"

    return result


# get project's config folder by file's path...
# -------------------------------------------------------
def get_project_config_path(file_name, project_name=C_COMMON_PROJECT_NAME):
    if project_name is None:
        project_name = C_COMMON_PROJECT_NAME

    common_path = get_project_root_path(file_name, project_name)

    # local folders structure: ../project/dags/config
    if is_test_mode():
        result = common_path + "/dags/config"
    # server folders structure: ..dags/env/project/config
    else:
        result = common_path + "/config"

    return result


# import file from path [module_file_path] and associate
# it with module [module_name]...
# -------------------------------------------------------
def import_source(module_file_path, module_name):
    logging.debug("[turel]: importing source {%s} -> {%s}" % (module_file_path, module_name))
    module_spec = import_util.spec_from_file_location(module_name, module_file_path)
    module = import_util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    return module


def import_source_new(module_file_path, module_name):
    logging.debug("[turel]: importing source new {%s} -> {%s}" % (module_file_path, module_name))
    module_path = "/".join(module_name.split(".")) + ".py"
    root_path = get_common_dag_path(module_file_path)
    import_path = root_path + "/" + module_path
    sys.path.insert(0, import_path)
    module = importlib.import_module(module_name)

    return module


def import_relative_source_new(file_name, module_name):
    logging.debug("[turel]: importing {%s} -> {%s}" % (file_name, module_name))

    module_path = "/".join(module_name.split(".")) + ".py"
    try:
        root_path = get_common_dag_path(file_name)
        return import_source_new(root_path + "/" + module_path, module_name)
    except FileNotFoundError:
        root_path = get_project_dag_path(file_name)
        return import_source(root_path + "/" + module_path, module_name)


# import [module_name] from the common library relatively
# to the [file_name] where import is called...
# -------------------------------------------------------
def import_relative_source(file_name, module_name):
    logging.debug("[turel]: importing {%s} -> {%s}" % (file_name, module_name))

    module_path = "/".join(module_name.split(".")) + ".py"
    try:
        root_path = get_common_dag_path(file_name)
        return import_source(root_path + "/" + module_path, module_name)
    except FileNotFoundError:
        root_path = get_project_dag_path(file_name)
        return import_source(root_path + "/" + module_path, module_name)


# import [module_name] from the common library relatively
# to the [file_name] where import is called...
# -------------------------------------------------------
def setup_paths(file_name):
    dir_name = get_common_root_path(file_name)
    if dir_name not in sys.path:
        logging.info("[turel]: path {%s} will be added to sys.path" % dir_name)
        sys.path.insert(0, dir)
