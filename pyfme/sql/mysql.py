from ..dataset.dataset_model import DatasetModel


class Mysql:
    def create_table(self, dataset: DatasetModel):
        names = dataset.table_names
        print(names)
