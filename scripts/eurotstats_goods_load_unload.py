import os
import requests
import gzip, io
import logging
import pandas as pd

logging.basicConfig()
logger = logging.getLogger(__name__)




def download(data_remote, intermediate_local, data_local):
    """
    download the remote data and clean up locally

    :param data_remote: Remote data link
    :type data_remote: str
    :param intermediate_local: intermediate files
    :type intermediate_local: str
    :param data_local: local file folder
    :type data_local: str
    """

    req = requests.get(data_remote)

    inter_cache = os.path.join(intermediate_local, "cache.gz")

    with open(inter_cache, "wb") as fp:
        fp.write(req.content)

    if os.path.isfile(inter_cache):
        logger.info(f"{inter_cache}")


    with gzip.GzipFile(inter_cache, "rb") as gz_b:
        data_content = gz_b.read()

    with open(data_local, 'wb') as fp:
        fp.write(data_content)


def parse_load_data(data_local, parsed_data_local):

    df = pd.read_csv(data_local, sep="\t")
    df.drop_duplicates(inplace=True)

    logger.info(f"{df.describe()}\n{df.info()}")

    lead_col = "unit,carriage,c_unload,geo\\time"

    # Split the first column
    df["country"] = df[lead_col].apply(lambda x: x.split(",")[-1])

    df["unload_country"] = df[lead_col].apply(lambda x: x.split(",")[-2])

    df["carriage"] = df[lead_col].apply(lambda x: x.split(",")[-3])
    df["unit"] = df[lead_col].apply(lambda x: x.split(",")[-4])

    UNITS = {
        "MIO_TKM": "million_tonne_km",
        "THS_T": "thousand_tonnes"
    }
    df.unit = df.unit.replace(UNITS)
    CARRIAGE = {
        "TOT": "total",
        "OWN": "own_account",
        "HIRE": "hire_or_reward",
        "NOT_SPEC": "not_specified"
    }
    df.carriage = df.carriage.replace(CARRIAGE)

    cols = [i for i in df.columns.tolist() if i != lead_col]
    info_cols = ["country", "unload_country", "carriage", "unit"]
    year_cols = [i for i in cols if i not in info_cols]

    # Melt the dataframe for easier pandas manipulations
    df_melted = pd.melt(df, id_vars=info_cols, value_vars=year_cols, var_name="year", value_name="value")

    # Sort the column order for a more readable output
    order_cols = [
        "country", "unload_country", "carriage", 'year', 'value', 'unit'
    ]
    df_melted = df_melted[order_cols]

    # Remove : in the values
    df_melted.value = df_melted.value.apply(lambda x: x.strip() if isinstance(x, str) else x)
    df_melted.replace({":": None, ": ": None}, inplace=True)
    df_melted.year = df_melted.year.apply(lambda x: x.strip())

    # Split into different measures
    df_tton = df_melted[df_melted.unit == "thousand_tonnes"]
    df_mtkm = df_melted[df_melted.unit == "million_tonne_km"]

    # export
    df_tton.to_csv(
        parsed_data_local["thousand_tonnes"], index=False
    )
    df_mtkm.to_csv(
        parsed_data_local["millon_tonne_km"], index=False
    )



def parse_unload_data(data_local, parsed_data_local):

    df = pd.read_csv(data_local, sep="\t")
    df.drop_duplicates(inplace=True)

    logger.info(f"{df.describe()}\n{df.info()}")

    lead_col = "unit,carriage,c_load,geo\\time"

    # Split the first column
    df["country"] = df[lead_col].apply(lambda x: x.split(",")[-1])

    df["load_country"] = df[lead_col].apply(lambda x: x.split(",")[-2])

    df["carriage"] = df[lead_col].apply(lambda x: x.split(",")[-3])
    df["unit"] = df[lead_col].apply(lambda x: x.split(",")[-4])

    UNITS = {
        "MIO_TKM": "million_tonne_km",
        "THS_T": "thousand_tonnes"
    }
    df.unit = df.unit.replace(UNITS)
    CARRIAGE = {
        "TOT": "total",
        "OWN": "own_account",
        "HIRE": "hire_or_reward",
        "NOT_SPEC": "not_specified"
    }
    df.carriage = df.carriage.replace(CARRIAGE)

    cols = [i for i in df.columns.tolist() if i != lead_col]
    info_cols = ["country", "load_country", "carriage", "unit"]
    year_cols = [i for i in cols if i not in info_cols]

    # Melt the dataframe for easier pandas manipulations
    df_melted = pd.melt(df, id_vars=info_cols, value_vars=year_cols, var_name="year", value_name="value")

    # Sort the column order for a more readable output
    order_cols = [
        "country", "load_country", "carriage", 'year', 'value', 'unit'
    ]
    df_melted = df_melted[order_cols]

    # Remove : in the values
    df_melted.value = df_melted.value.apply(lambda x: x.strip() if isinstance(x, str) else x)
    df_melted.replace({":": None, ": ": None}, inplace=True)
    df_melted.year = df_melted.year.apply(lambda x: x.strip())

    # Split into different measures
    df_tton = df_melted[df_melted.unit == "thousand_tonnes"]
    df_mtkm = df_melted[df_melted.unit == "million_tonne_km"]

    # export
    df_tton.to_csv(
        parsed_data_local["thousand_tonnes"], index=False
    )
    df_mtkm.to_csv(
        parsed_data_local["millon_tonne_km"], index=False
    )



if __name__ == "__main__":

    DATA_REMOTE = {
        "load": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Froad_go_iq_ltt.tsv.gz",
        "unload": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Froad_go_iq_utt.tsv.gz"
    }
    CACHE_FOLDER = "/tmp"

    DATA_LOCAL = {
        "load": "/tmp/load.tsv",
        "unload": "/tmp/unload.tsv"
    }
    PARSED_DATA_LOCAL = {
        "load": {
            "thousand_tonnes": "dataset/road_freight_goods_loaded_in_country_in_thousand_tonnes.csv",
            "millon_tonne_km": "dataset/road_freight_goods_loaded_in_country_in_million_tonne_km.csv"
        },
        "unload": {
            "thousand_tonnes": "dataset/road_freight_goods_unloaded_in_country_in_thousand_tonnes.csv",
            "millon_tonne_km": "dataset/road_freight_goods_unloaded_in_country_in_million_tonne_km.csv"
        }
    }

    # Unload
    download(DATA_REMOTE['unload'], CACHE_FOLDER, DATA_LOCAL['unload'])
    parse_unload_data(DATA_LOCAL['unload'], PARSED_DATA_LOCAL['unload'])

    # Load
    download(DATA_REMOTE['load'], CACHE_FOLDER, DATA_LOCAL['load'])
    parse_load_data(DATA_LOCAL['load'], PARSED_DATA_LOCAL['load'])

    pass