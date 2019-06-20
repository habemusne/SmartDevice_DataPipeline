class Resource:
    def __init__(self, **kwargs):
        self._data_name = kwargs.get('data_name')

    def create(self, **kwargs):
        raise NotImplementedError

    def delete(self, **kwargs):
        raise NotImplementedError
