import json
import os
from io import StringIO
from pathlib import Path

import httpx
import pandas as pd

GPW_LINK = "https://www.gpw.pl/archiwum-notowan-full?type=66&instrument=&date={}"


def get_option_prices(start: str = "2002-01-01", end: str = "2024-01-01") -> pd.DataFrame:
    """
    get option prices from GPW

    make requests to GPW archive for option prices


    Parameters
    ----------
    start : str, optional
        start date, by default "2002-01-01"
    end : str, optional
        end date, by default "2024-01-01"

    Returns
    -------
    pd.DataFrame
        complete dataframe
    """
    dates = pd.date_range(start, end, freq="B")
    dates = [date.strftime("%d-%m-%Y") for date in dates]
    print("links prepared")

    responses = []
    i = 0

    # async requests are quickly blocked :(
    with httpx.Client() as client:
        while i < len(dates):
            date = dates[i]
            try:
                print(date)
                resp = client.get(GPW_LINK.format(date))
                responses.append((date, resp))
                i += 1
            except httpx.ReadTimeout as e:
                print(f"{e} for {date}")
                continue

    master = pd.DataFrame()

    for date, reponse in responses:
        try:
            dfs = pd.read_html(StringIO(reponse.text))

            options = dfs[1]
            options["DATE"] = date

            master = pd.concat([master, options])

        except Exception:
            # print(f"{date} has only one df")
            continue

    master.to_csv("option_prices.csv")
    return master


def preprocess_greeks() -> pd.DataFrame:
    """
    preprocess csv files with greeks for options

    you can download greeks for WIG20 options with the link below
    https://www.gpw.pl/ajaxindex.php?action=DRGreek&start=archive&format=html&lang=PL
    this function helps to preprocess the data, which includes:
    - combining files
    - renaming cols
    - unpacking ticker


    Returns
    -------
    pd.DataFrame
        complete dataframe
    """
    greeks_csvs = os.listdir("option_greeks")
    master = pd.DataFrame()
    for file in greeks_csvs:
        df = pd.read_csv(
            Path("option_greeks", file), encoding="windows-1250", delimiter=";", decimal=","
        )
        master = pd.concat([master, df])

    master = master.dropna(axis=1)
    rn = {
        "Data": "date",
        "Nazwa": "ticker",
        "Zmienność implikowana": "implied_volatility",
        "Zmienność": "volatility",
        "Stopa procentowa": "rfr_rate",
        "Stopa dywidendy": "div_rate",
        "Delta": "delta",
        "Gamma": "gamma",
        "Theta": "theta",
        "Vega": "vega",
        "Rho": "rho",
    }
    master = master.rename(columns=rn)
    master.date = pd.to_datetime(master.date)

    with open("option_codes.json") as f:
        codes = json.load(f)

    codes = pd.DataFrame(codes).T.reset_index()
    codes = codes.astype({"maturity_month": str})

    master["underlying"] = master.ticker.str[1:4]
    master["code"] = master.ticker.str[4]

    master = pd.merge(
        master, codes, how="left", left_on="code", right_on="index", validate="many_to_one"
    )
    master = master.drop(columns=["index"])

    master["year_strike"] = master["ticker"].str[5:]

    # temporary cols for easier parsing
    master["t"] = master.year_strike.str.len()
    master["y"] = master.year_strike.str[0].astype(int)

    # old options
    # before 2014 options have 4 numbers after the one letter code
    # they treat the first number as the maturity year
    # when the year is past 2010 it become 0, 1, 2, 3, 4
    # so it's necessary to indicate that by concating with "1"
    mask = (master.t == 4) & (master.y < 6)
    master.loc[mask, "maturity_year"] = "1" + master.year_strike.str[0]
    master.loc[mask, "strike"] = master.year_strike.str[1:]

    mask = (master.t == 4) & (master.y >= 6)
    master.loc[mask, "maturity_year"] = master.year_strike.str[0]
    master.loc[mask, "strike"] = master.year_strike.str[1:]

    # new options
    # after 2014 options have code 6 numbers after the one letter code
    mask = master.t == 6
    master.loc[mask, "maturity_year"] = master.year_strike.str[:2]
    master.loc[mask, "strike"] = master.year_strike.str[2:]

    # parse date and make them 3rd friday of that month
    # unfortunately not vectorized
    master["maturity"] = master.maturity_year + "-" + master.maturity_month + "-01"
    master["maturity"] = pd.to_datetime(master.maturity, yearfirst=True)

    # third friday is week=2, weekday=4
    master["maturity"] = master["maturity"] + pd.offsets.WeekOfMonth(week=2, weekday=4)

    master = master.drop(columns=["year_strike", "maturity_month", "maturity_year", "y", "t"])

    master = master.astype(
        {
            "strike": int,
            "type": "category",
        }
    )

    master.to_csv("option_greeks.csv")

    return master


if __name__ == "__main__":
    pass
