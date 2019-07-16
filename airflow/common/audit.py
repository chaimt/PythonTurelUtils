import logging
import uuid


def init_dag():
    logging.info('init_dag')


def teardown_dag():
    logging.info('teardown_dag')


def add_request_id(kwargs):
    if 'request_id' not in kwargs:
        kwargs['request_id'] = str(uuid.uuid4())[:8]
    return kwargs['request_id']


def turel_log_builder(text, kwargs):
    request_id = add_request_id(kwargs)
    dag_id = None
    dag = kwargs['dag']
    if dag is not None:
        dag_id = dag.dag_id
    return '[turel]: dag_id={dag_id}, request_id={request_id}, {txt}'.format(dag_id=dag_id, request_id=request_id,
                                                                              txt=text.format(kwargs))


def turel_log_error_ex(text, kwargs):
    logging.error(turel_log_builder(text, kwargs))


def turel_log_ex(text, kwargs):
    logging.info(turel_log_builder(text, kwargs))


def turel_log_warning_ex(text, kwargs):
    logging.warning(turel_log_builder(text, kwargs))


def turel_log_debug_ex(text, kwargs):
    logging.debug(turel_log_builder(text, kwargs))


def turel_log_exception_ex(text, kwargs):
    logging.exception(turel_log_builder(text, kwargs))


def turel_log(text):
    logging.info('[turel]: ' + text)


def turel_log_error(text):
    logging.error('[turel]: ' + text)


def turel_log_warning(text):
    logging.warning('[turel]: ' + text)


def turel_log_debug(text):
    logging.debug('[turel]: ' + text)


def turel_log_exception(text):
    logging.exception('[turel]: ' + text)


def turel_log_error_if_false(test, error_msg):
    if not test:
        turel_log_ex(error_msg)
    return test
