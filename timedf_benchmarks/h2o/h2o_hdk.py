import warnings

import pyhdk


hdk = pyhdk.init()

from .h2o_utils import H2OBackend


def groupby_q1(x):
    return x.agg("id1", v1="sum(v1)").run()


def groupby_q2(x):
    return x.agg(["id1", "id2"], v1="sum(v1)").run()


def groupby_q3(x):
    return x.agg("id3", v1="sum(v1)", v3="avg(v3)").run()


def groupby_q4(x):
    return x.agg("id4", v1="avg(v1)", v2="avg(v2)", v3="avg(v3)").run()


def groupby_q5(x):
    return x.agg("id6", v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run()


def groupby_q6(x):
    return x.agg(
        ["id4", "id5"], v3_median="approx_quantile(v3, 0.5)", v3_stddev="stddev(v3)"
    ).run()


def groupby_q7(x):
    tmp = x.agg("id3", "max(v1)", "min(v2)")
    return tmp.proj("id3", range_v1_v2=tmp["v1_max"] - tmp["v2_min"]).run()


def groupby_q8(x):
    tmp = x.proj(
        "id6",
        "v3",
        row_no=hdk.row_number().over(x.ref("id6")).order_by((x.ref("v3"), "desc")),
    )
    return tmp.filter(tmp.ref("row_no") < 3).proj("id6", "v3").run()


def groupby_q9(x):
    tmp = x.agg(["id2", "id4"], r2="corr(v1, v2)")
    return tmp.proj(r2=tmp["r2"] * tmp["r2"]).run()


def groupby_q10(x):
    return x.agg(
        ["id1", "id2", "id3", "id4", "id5", "id6"], v3="sum(v3)", v1="count"
    ).run()


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
    return data["df"].join(data["small"], "id1").run()


def join_q2(data):
    return data["df"].join(data["medium"], "id2").run()


def join_q3(data):
    return data["df"].join(data["medium"], "id2", how="left").run()


def join_q4(data):
    return data["df"].join(data["medium"], "id5").run()


def join_q5(data):
    return data["df"].join(data["big"], "id3").run()


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
                **{n: "dict" for n in ["id1", "id2", "id3"]},
                **{n: "int32" for n in ["id4", "id5", "id6", "v1", "v2"]},
                "v3": "fp64",
            },
            "left": {
                **{n: "int32" for n in ["id1", "id2", "id3"]},
                **{n: "dict" for n in ["id4", "id5", "id6"]},
                "v1": "fp64",
            },
            "right_small": {"id1": "int32", "id4": "dict", "v2": "fp64"},
            "right_medium": {
                **{n: "int32" for n in ["id1", "id2"]},
                **{n: "dict" for n in ["id4", "id5"]},
                "v2": "fp64",
            },
            "right_big": {
                **{n: "int32" for n in ["id1", "id2", "id3"]},
                **{n: "dict" for n in ["id4", "id5", "id6"]},
                "v2": "fp64",
            },
        }

    def load_groupby_data(self, paths):
        return hdk.import_csv(str(paths["groupby"]), schema=self.dtypes["groupby"])

    def load_join_data(self, paths):
        df = hdk.import_csv(str(paths["join_df"]), schema=self.dtypes["left"])
        small = hdk.import_csv(
            str(paths["join_small"]), schema=self.dtypes["right_small"]
        )
        medium = hdk.import_csv(
            str(paths["join_medium"]), schema=self.dtypes["right_medium"]
        )
        big = hdk.import_csv(str(paths["join_big"]), schema=self.dtypes["right_big"])

        return {"df": df, "small": small, "medium": medium, "big": big}
