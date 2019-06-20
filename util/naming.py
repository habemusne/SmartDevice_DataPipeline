def topic_name(data_name):
    return '_'.join('topic', data_name)


def jdbc_topic_prefix():
    return 'topic_autogen_'


def stream_name(data_name):
    return '_'.join('stream', data_name)


def table_name(data_name):
    return '_'.join('table', data_name)


def topic_name_jdbc(data_name):
    return jdbc_topic_prefix() + 'data_name'


def connector_name(data_name):
    return '_'.join('connect', data_name()
