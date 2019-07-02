from util.logger import logger


class Resource:
    def __init__(self, **kwargs):
        self._data_name = kwargs.get('data_name', '')

    def create(self, **kwargs):
        raise NotImplementedError

    def delete(self, **kwargs):
        raise NotImplementedError

    def log_notify(func):
        def wrapper(*args, **kwargs):
            action, data_name, obj_type = func.__name__, args[0]._data_name, args[0].__class__.__name__
            logger.info('{} {} {} in progress...'.format(action, data_name, obj_type.lower()))
            result = func(*args, **kwargs)
            logger.info('{} {} {} done.'.format(action, data_name, obj_type.lower()))
            return result
        return wrapper
