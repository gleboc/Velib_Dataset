#!/usr/bin/env python3
"""
Small script to extract data from the paris opendata velib API
and save it as CSV files
"""

import pandas as pd
import requests
import time

def get_stations():
    sapi="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
    stations_df = pd.DataFrame(requests.get(sapi).json()["data"]["stations"])
    stations_df.set_index("stationCode", inplace=True)
    stations_df.sort_index(inplace=True)
    stations_df['station_geo'] = ["{:.5f},{:.5f}".format(s.lat, s.lon) for s in stations_df.itertuples()]
    stations_df['credit_card'] = stations_df.rental_methods.str.contains('CREDITCARD', regex=False).fillna(False)
    stations_df = stations_df.rename(columns={"name": 'station_name'})
    return stations_df[['station_name', 'capacity', 'station_geo', 'credit_card']]


def get_statuses():
    sapi = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
    j = requests.get(sapi).json()["data"]["stations"]
    df = pd.DataFrame(j)
    df = df.astype({'is_renting': bool, 'stationCode': str, 'last_reported': 'datetime64[s]'})
    df = df.join(df["num_bikes_available_types"].apply(lambda l: pd.Series({**l[0], **l[1]})))

    df.rename({
        "mechanical": "available_mechanical",
        "ebike": "available_electrical",
        "is_renting": "operative",
        "last_reported": "last_update",
    }, axis='columns', inplace=True)
    df["time_processed"] = pd.Timestamp("now", tz='UTC')
    return df[['stationCode', 'station_id', 'num_docks_available', 'operative', 'last_update',
               'available_mechanical', 'available_electrical', 'time_processed']]  # 'is_installed', 'is_returning',


def main():
    get_statuses().to_csv('historique_stations_v2.csv', header=False, mode='a', date_format='%Y-%m-%dT%H:%MZ')


if __name__ == "__main__":
    # get_stations().to_csv('stations_details.csv', header=False, mode='a', date_format='%Y-%m-%dT%H:%MZ')
    freq = 2  # every 2 minutes
    t = round(time.time())
    while True:
        t = t + freq * 60
        main()
        time.sleep(max(0, t - time.time()))

