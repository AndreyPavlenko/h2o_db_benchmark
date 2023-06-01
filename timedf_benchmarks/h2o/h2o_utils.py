import abc
from pathlib import Path


def filter_dict(d, names):
    return {key: val for key, val in d.items() if key in names}


class H2OBackend(abc.ABC):
    name2groupby_query = None
    name2join_query = None

    @abc.abstractmethod
    def load_groupby_data(self, paths):
        pass

    @abc.abstractmethod
    def load_join_data(self, paths):
        pass


def get_load_info(data_path, size: str):
    data_path = Path(data_path)

    def join_to_tbls(data_name):
        x_n = int(float(data_name.split("_")[1]))
        y_n = ["{:.0e}".format(x_n / e) for e in [1e6, 1e3, 1]]
        y_n = [data_name.replace("NA", y).replace("+0", "") for y in y_n]
        return y_n

    file_name = {
        "small": "1_1e7_NA_0_0",
        "medium": "1_1e8_NA_0_0",
        "large": "1_1e9_NA_0_0",
    }[size]
    paths = [data_path / f"J{f}.csv" for f in [file_name, *join_to_tbls(file_name)]]

    return {
        "groupby": data_path / f"G{file_name.replace('NA', '1e2')}.csv",
        "join_df": paths[0],
        "join_small": paths[1],
        "join_medium": paths[2],
        "join_big": paths[3],
    }
