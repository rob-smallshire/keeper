from abc import ABC, abstractmethod


class Storage(ABC):

    @abstractmethod
    def keys(self):
        """An iterator over all keys
        """
        raise NotImplementedError

    @abstractmethod
    def openout_meta(self, key):
        raise NotImplementedError

    @abstractmethod
    def openin_meta(self, key):
        raise NotImplementedError

    @abstractmethod
    def openout_data(self, key):
        raise NotImplementedError

    def openin_data(self, key):
        raise NotImplementedError

    @abstractmethod
    def openout_temp(self):
        raise NotImplementedError

    @abstractmethod
    def openin_temp(self, handle):
        raise NotImplementedError

    @abstractmethod
    def promote_temp(self, handle, key):
        raise NotImplementedError

    @abstractmethod
    def remove_temp(self, handle):
        raise NotImplementedError

    @abstractmethod
    def remove(self, key):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def closed(self):
        raise NotImplementedError
