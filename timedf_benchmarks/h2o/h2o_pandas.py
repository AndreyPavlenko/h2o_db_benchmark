import warnings
import time

from timedf.backend import pd, Backend


from .h2o_utils import H2OBackend

# Without {"observed": True}, pandas fails groupby_q10 because of memory problem
# Looks like it builds cartesian product for all categorical values and there are too many of them.
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html
gb_params = {"as_index": False, "observed": True, "sort": False}


def groupby_q1(x):
    return x.groupby("id1", **gb_params).agg({"v1": "sum"})


def groupby_q2(x):
    return x.groupby(["id1", "id2"], **gb_params).agg({"v1": "sum"})


def groupby_q3(x):
    return x.groupby("id3", **gb_params).agg({"v1": "sum", "v3": "mean"})


def groupby_q4(x):
    return x.groupby("id4", **gb_params).agg({"v1": "mean", "v2": "mean", "v3": "mean"})


def groupby_q5(x):
    return x.groupby("id6", **gb_params).agg({"v1": "sum", "v2": "sum", "v3": "sum"})


def groupby_q6(x):
    return x.groupby(["id4", "id5"], **gb_params).agg({"v3": ["median", "std"]})


def groupby_q7(x):
    return (
        x.groupby("id3", **gb_params)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["id3", "range_v1_v2"]]
    )


def groupby_q8(x):
    return (
        x[~x["v3"].isna()][["id6", "v3"]]
        .sort_values("v3", ascending=False)
        .groupby("id6", **gb_params)
        .head(2)
    )


def groupby_q9(x):
    if Backend.get_name() == "Modin_on_hdk":
        from modin.experimental.sql import query

        sql = """
        SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2
        FROM df
        GROUP BY id2, id4;
        """
        return query(sql, df=x)

    return (
        x[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], **gb_params)
        .apply(lambda x: pd.Series({"r2": x["v1"].corr(x["v2"]) ** 2}))
    )


def groupby_q10(x):
    return x.groupby(["id1", "id2", "id3", "id4", "id5", "id6"], **gb_params).agg(
        {"v3": "sum", "v1": "size"}
    )


name2groupby_query = {
    "q01": groupby_q1,
    "q02": groupby_q2,
    "q03": groupby_q3,
    "q04": groupby_q4,
    "q05": groupby_q5,
    "q06": groupby_q6,
    "q07": groupby_q7,
    "q08": groupby_q8,
    "q09": groupby_q9,
    "q10": groupby_q10,
}


def join_q1(data):
    return data["df"].merge(data["small"], on="id1")


def join_q2(data):
    return data["df"].merge(data["medium"], on="id2")


def join_q3(data):
    return data["df"].merge(data["medium"], how="left", on="id2")


def join_q4(data):
    return data["df"].merge(data["medium"], on="id5")


def join_q5(data):
    return data["df"].merge(data["big"], on="id3")


class H2OBackendImpl(H2OBackend):
    name2groupby_query = name2groupby_query
    name2join_query = {
        "q01": join_q1,
        "q02": join_q2,
        "q03": join_q3,
        "q04": join_q4,
        "q05": join_q5,
    }

    def __init__(self, modin_exp_gb):
        self.dtypes = {
            "groupby": {
                **{n: "category" for n in ["id1", "id2", "id3"]},
                **{n: "int32" for n in ["id4", "id5", "id6", "v1", "v2"]},
                "v3": "float64",
            },
            "left": {
                **{n: "int32" for n in ["id1", "id2", "id3"]},
                **{n: "category" for n in ["id4", "id5", "id6"]},
                "v1": "float64",
            },
            "right_small": {"id1": "int32", "id4": "category", "v2": "float64"},
            "right_medium": {
                **{n: "int32" for n in ["id1", "id2"]},
                **{n: "category" for n in ["id4", "id5"]},
                "v2": "float64",
            },
            "right_big": {
                **{n: "int32" for n in ["id1", "id2", "id3"]},
                **{n: "category" for n in ["id4", "id5", "id6"]},
                "v2": "float64",
            },
        }

        # Activate experimental groupby
        if modin_exp_gb and Backend.get_name() == "Modin_on_ray":
            import modin

            if hasattr(modin.config, "ExperimentalGroupbyImpl"):
                modin.config.ExperimentalGroupbyImpl.put(True)

    def load_groupby_data(self, paths):
        return pd.read_csv(paths["groupby"], dtype=self.dtypes["groupby"])

    def load_join_data(self, paths):
        df = pd.read_csv(paths["join_df"], dtype=self.dtypes["left"])
        small = pd.read_csv(paths["join_small"], dtype=self.dtypes["right_small"])
        medium = pd.read_csv(paths["join_medium"], dtype=self.dtypes["right_medium"])
        big = pd.read_csv(paths["join_big"], dtype=self.dtypes["right_big"])

        return {"df": df, "small": small, "medium": medium, "big": big}
