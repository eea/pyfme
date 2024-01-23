from abc import ABC, abstractmethod

# from .dataset import DataSet


class QualityTest(ABC):
    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @abstractmethod
    def check(self, dataset):
        pass
