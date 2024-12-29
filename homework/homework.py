"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
from glob import glob

import numpy as np
import pandas as pd  # type: ignore


def load_data(input_directory):
    """Read zip files in a directory and store them in seq"""
    dfs = []

    routes = glob(f"{input_directory}/*")

    for route in routes:
        with zipfile.ZipFile(f"{route}", mode="r") as zf:
            for fn in zf.namelist():

                with zf.open(fn) as f:
                    dfs.append(pd.read_csv(f, sep=",", index_col=0))

    return pd.concat(dfs, ignore_index=True)


def client_data(df):
    """Takes the dfs and segments the client information

    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0
    """
    client_df = df.copy()

    client_df = client_df[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ]

    client_df["job"] = client_df["job"].str.replace(".", "")
    client_df["job"] = client_df["job"].str.replace("-", "_")

    client_df["education"] = client_df["education"].str.replace(".", "_")
    client_df["education"] = client_df["education"].replace("unknown", np.nan)

    client_df["credit_default"] = client_df["credit_default"].apply(
        lambda x: 1 if x == "yes" else 0
    )
    client_df["mortgage"] = client_df["mortgage"].apply(
        lambda x: 1 if x == "yes" else 0
    )

    return client_df


def campaign_data(df):
    """Takes the dfs and segments the campaign information

    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    """

    months = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    campaign_df = df.copy()

    campaign_df = campaign_df[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            # Add last contact day
        ]
    ]

    campaign_df["last_contact_date"] = (
        "2022-" + df["month"].map(months) + "-" + df["day"].astype(str)
    )

    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(
        lambda x: 1 if x == "success" else 0
    )

    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(
        lambda x: 1 if x == "yes" else 0
    )

    print(campaign_df.shape)
    return campaign_df


def economics_data(df):
    """Takes the dfs and segments the econ information

    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    econ_df = df.copy()

    econ_df = econ_df[["client_id", "cons_price_idx", "euribor_three_months"]]

    return econ_df


def _create_ouptput_directory(output_directory):
    if os.path.exists(output_directory):
        for file in glob(f"{output_directory}/*"):
            os.remove(file)
        os.rmdir(output_directory)
    os.makedirs(output_directory)


def _save_output(output_directory, filename, df):
    df.to_csv(f"{output_directory}/{filename}.csv", index=False)


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    df = load_data("files/input")

    client_df = client_data(df)
    campaign_df = campaign_data(df)
    econ_df = economics_data(df)

    _create_ouptput_directory("files/output")
    _save_output("files/output", "client", client_df)
    _save_output("files/output", "campaign", campaign_df)
    _save_output("files/output", "economics", econ_df)

    return 1


if __name__ == "__main__":
    clean_campaign_data()
