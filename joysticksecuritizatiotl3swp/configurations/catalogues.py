"""
This module contains the hardcoded catalogues the process uses
"""
from rubik.config.catalogues.countries_catalogue import Countries_Cat
from rubik.constant.rubik_constants import RubikConstants as RC

rating_dict = {
    "AAA": "01",
    "AA+": "02",
    "AA": "03",
    "AA-": "04",
    "A+": "05",
    "A": "06",
    "A-": "07",
    "BBB+": "08",
    "BBB+1": "09",
    "BBB+2": "10",
    "BBB": "11",
    "BBB1": "12",
    "BBB2": "13",
    "BBB-": "14",
    "BBB-1": "15",
    "BBB-2": "16",
    "BB+": "17",
    "BB+1": "18",
    "BB+2": "19",
    "BB": "20",
    "BB1": "21",
    "BB2": "22",
    "BB-": "23",
    "BB-1": "24",
    "BB-2": "25",
    "B+": "26",
    "B+1": "27",
    "B+2": "28",
    "B+3": "29",
    "B": "30",
    "B1": "31",
    "B2": "32",
    "B3": "33",
    "B-": "34",
    "B-1": "35",
    "B-2": "36",
    "B-3": "37",
    "CCC+": "38",
    "CCC": "39",
    "CCC-": "40",
    "CC+": "41",
    "CC": "42",
    "CC-": "43",
    "C+": "44",
    "C": "45",
    "C-": "46",
    "D": "47",
    "D1": "48",
    "D2": "49",
    "No Rating": "50",
    "XXXX": "50",
}

tier_dict = RC.Tiers.tier_ids

segment_dict = {
    "00001": "Institutional Client",
    "00002": "Public Sector",
    "00003": "Sme & Retail",
    "00004": "Others",
    "00005": "Corporate",
    "00006": "Financial Sponsors",
}

# CONFIGURACIONES DE TIPOS DE MOVIMIENTOS
mov_evol_saldos = [
    "Authorized Amount Increase",
    "Authorized Amount Reduction",
    "Drawdown",
    "Repayment of Principal",
]

# grupos de movimientos para calculos posteriores
listaMov = [
    "Authorized Amount Reduction for Irregular Payment",
    "Early Authorized Amount Reduction",
    "Non-Revolving Guarantee Reduction",
    "RCF Mandatory Early Repayment",
    "RCF Total Early Repayment",
    "Revolving Guarantee Drawdown Cancellation",
    "Revolving Guarantee Drawdown Early Cancellation",
    "Term Loan Mandatory Early Repayment",
    "Term Loan Repayment",
    "Term Loan Total Early Repayment",
    "Term Loan Voluntary Early Repayment",
    "Voluntary RCF Early Repayment",
]

listaMov1 = [
    "Drawdown Increase",
    "Non-Revolving Guarantee Drawdown",
    "RCF Drawdown",
    "Revolving Guarantee Drawdown",
    "Revolving Guarantee Increase",
    "Term Loan Drawdown",
]

listaMov2 = [
    "Non-Revolving Guarantee Reduction",
    "Non-Revolving Guarantee Drawdown Cancellation",
    "Non-Revolving Guarantee Drawdown Early Cancellation",
    "RCF Mandatory Early Repayment",
    "RCF Refund",
    "RCF Refund#",
    "RCF Total Early Repayment",
    "Revolving Guarantee Drawdown Cancellation",
    "Revolving Guarantee Drawdown Early Cancellation",
    "Revolving Guarantee Reduction",
    "Term Loan Mandatory Early Repayment",
    "Term Loan Refund",
    "Term Loan Repayment",
    "Term Loan Total Early Repayment",
    "Term Loan Voluntary Early Repayment",
    "Voluntary RCF Early Repayment",
]

listaMov4 = [
    "RCF Refund#",
    "Revolving Guarantee Drawdown Cancellation",
    "Term Loan Repayment",
]


def get_countries_iso_name_table(spark):  # pragma: no cover
    """
    This method returns the table containing the country iso2 code and the country names
    to be persisted in the generic data module
    """
    countries = Countries_Cat().dict_iso_country
    countries = spark.createDataFrame(countries.items(), ["country_id", "country_desc"])
    return countries
