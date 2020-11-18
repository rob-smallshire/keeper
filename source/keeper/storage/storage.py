from abc import ABC, abstractmethod


class Storage(ABC):

    @abstractmethod
    def open_meta(self, key, mode):
        raise NotImplementedError

    @abstractmethod
    def openout_data(self, key):
        raise NotImplementedError

    def openin_data(self, key):
        raise NotImplementedError

    @abstractmethod
    def create_temp(self):
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
