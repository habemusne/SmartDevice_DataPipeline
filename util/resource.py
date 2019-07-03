from util.logger import logger


class Resource:
    def create(self, **kwargs):
        raise NotImplementedError

    def delete(self, **kwargs):
        raise NotImplementedError

    def log_notify(func):
        def wrapper(*args, **kwargs):
            action, name, obj_type = func.__name__, args[0].name, args[0].__class__.__name__
            logger.info('{} {} {} in progress...'.format(action, obj_type.lower(), name))
            result = func(*args, **kwargs)
            logger.info('{} {} {} done.'.format(action, obj_type.lower(), name))
            return result
        return wrapper
