__author__ = 'Yijun Lou, Hao Ren, Zhanjie Shen, Zhenqin Yuan, Dizhou Wu, Shengdong Yang, Jiayong Huang'
__copyright__ = "Copyright 2019, The Group Project of CME"
__credits__ = ["Yijun Lou", "Hao Ren", "Zhanjie Shen", "Zhenqin Yuan", "Dizhou Wu", "Shengdong Yang", "Jiayong Huang"]
__license__ = "University of Illinois, Urbana Champaign"
__version__ = "1.0.0"

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os

working_dir = os.getcwd()


def get_one_contract_data_by_date(date_str, contract_pd):
    """Get the contract minute data by the date
    Trading hours:
        Sunday – Friday, 7:00 p.m. – 7:45 a.m. CT and
        Monday – Friday, 8:30 a.m. – 1:20 p.m. CT
    Define the last day's night-time and today's day-time as today's trading hours

    Args:
        date_str: str, the date we want to extract the main contract data, e.g. "2016-01-04"
        contract_path: str, the main contract path, e.g. "../data/ZC/ZCH16.csv"

    Returns:
        pandas.DataFrame

    """
    year, month, day = map(int, date_str.split('-'))
    last_date_str = (datetime(year, month, day) - timedelta(days=1)).strftime("%Y-%m-%d")

    all_data = contract_pd
    all_data['Volume'] = all_data.TotalVolume.diff()

    last_day_data = all_data.loc[last_date_str].between_time("19:00", "23:59")
    today_data = all_data.loc[date_str].between_time("0:00", "13:20")

    data = last_day_data.append(today_data, ignore_index=False)
    data.iloc[0, 5] = data.iloc[0, 4]  # The first minute's trading volume
    return data

def extract_data_from_unique_contract(contract_path, unique_dates):
    '''
    Read the ticks of the contract file and combine them into one object by dates.
    :param contract_path: str, the absolute path of contract file
    :param unique_dates: arr, the unique dates in the contract front file
    :return: pandas.DataFrame, the combined data of all unique dates
    '''
    contract_pd = pd.read_csv(contract_path, header=0, index_col=0, parse_dates=[0])
    data = pd.DataFrame(columns=["Open", "High", "Low", "Close", "TotalVolume", "Volume"])
    for single_date in unique_dates:
        dd = get_one_contract_data_by_date(single_date, contract_pd)
        data = data.append(dd, ignore_index=False)
    return data


def get_all_main_contract_data(main_contracts_path=working_dir + "/data/ZC/front.csv",
                               contracts_root_path=working_dir + "/data/ZC/"):
    """Combine the daily main contract data
    Args:
        main_contracts_path: str, the path that have the daily main contract code
        contracts_root_path: str, the root path that have all the contract data

    Returns:
        data, pd.DataFrame,columns=["Open", "High", "Low", "Close", "TotalVolume", "Volume", "Change"],
                            where "Change" == 1 indicate that the main contract changed that day

    """
    func_contract_to_path = lambda x: contracts_root_path + "ZC" + x + ".csv"

    main_contracts = pd.read_csv(main_contracts_path, header=None, names=['date_str', 'main_contract'])
    unique_contracts = main_contracts.main_contract.unique()

    data = pd.DataFrame(columns=["Open", "High", "Low", "Close", "TotalVolume", "Volume"])
    for single_contract in unique_contracts:
        contract_path = func_contract_to_path(single_contract)
        unique_dates = main_contracts.loc[main_contracts["main_contract"] == single_contract].date_str.unique()
        today_data = extract_data_from_unique_contract(contract_path, unique_dates)
        data = data.append(today_data, ignore_index=False)

    return data

def save_data_to_path(all_data, data_root_path = working_dir + "/data/combined_data.csv"):
    all_data.to_csv(data_root_path)


def get_sub_data_between_time_interval(all_data,
                                       start_date="2016-01-03",
                                       start_time_in_day="19:00:00",
                                       end_date="2019-03-01",
                                       end_time_in_day="13:20:00",
                                       include=True):
    '''

    :param all_data: pandas.DataFrame, the whole data set of transactions.
    :param start_date: str, date of transaction starts.
    :param start_time_in_day: str, time of transaction starts.
    :param end_date: str, date of transaction ends.
    :param end_time_in_day: str, time of transaction ends.
    :param include: boolean, True if includes the start time and end time.
    :return: pandas.DataFrame, a subset of all_data object with specific time interval.
    '''
    start_index = start_date + " " + start_time_in_day
    end_index = end_date + " " + end_time_in_day
    t_interval = (all_data.index >= start_index) & (all_data.index <= end_index) \
        if include else \
        (all_data.index > start_index) & (all_data.index < end_index)
    sub_data = all_data.loc[t_interval]
    return sub_data


def get_sub_data_in_no_of_transactions(all_data,
                                       start_date="2016-01-03",
                                       start_time_in_day="19:00:00",
                                       no_of_trans=10000,
                                       include=True):
    '''

    :param all_data: pandas.DataFrame, the whole data set of transactions.
    :param start_date: str, date of transaction starts.
    :param start_time_in_day: str, time of transaction starts.
    :param no_of_trans: int, number of transactions to keep
    :param include: boolean, True if includes the start time
    :return: pandas.DataFrame, a subset of all_data object with specific number of transactions.
    '''
    start_index = start_date + " " + start_time_in_day
    t_interval = (all_data.index >= start_index) \
        if include else \
        (all_data.index > start_index)
    sub_data = all_data.loc[t_interval][:no_of_trans]
    return sub_data


def get_combined_sub_data_by_period(all_data,
                                    start_date="2016-01-03",
                                    start_time_in_day="19:00:00",
                                    end_date="2019-03-01",
                                    end_time_in_day="13:20:00",
                                    period="D",
                                    include=True):
    '''

    :param all_data: pandas.DataFrame, the whole data set of transactions.
    :param start_date: str, date of transaction starts.
    :param start_time_in_day: str, time of transaction starts.
    :param end_date: str, date of transaction ends.
    :param end_time_in_day: str, time of transaction ends.
    :param period: str, indicates different period of time, "D" - daily, "W" - weekly, "M" - Monthly, "Y" - Yearly.
    :param include: boolean, True if includes the start time and end time.
    :return: pandas.DataFrame, a collection of all_data object with specific periods of transactions.
    '''
    start_index = start_date + " " + start_time_in_day
    end_index = end_date + " " + end_time_in_day
    t_interval = (all_data.index >= start_index) & (all_data.index <= end_index) \
        if include else \
        (all_data.index > start_index) & (all_data.index < end_index)
    sub_data = all_data.loc[t_interval]

    sub_data = sub_data.between_time('19:00', '13:20', include_start=True, include_end=True)
    proxy = sub_data.index + pd.DateOffset(hours=5)
    combined_data = sub_data.groupby(proxy.date).agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'TotalVolume': 'last'})
    combined_data = combined_data.reindex(columns=['Open', 'High', 'Low', 'Close', 'TotalVolume'])
    combined_data.index = pd.DatetimeIndex(combined_data.index)
    return combined_data.resample(period).agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'TotalVolume': 'sum'})

def get_stat_val_between_time_interval(sub_data,
                                       attribute="Close",
                                       stat_method="Mean"):
    '''

    :param sub_data: pandas.DataFrame, the whole data set to be analyzed.
    :param attribute: str, the attribute that in sub_data object that to be analyzed.
    :param stat_method: str, indicator of which method to use for analyzing part.
    :return: numerical value or series.
    '''
    stat_dict = {
        "Mean": sub_data[attribute].mean(),
        "SD": sub_data[attribute].std(),
        "Diff": sub_data[attribute].diff(),
        "P_change": sub_data[attribute].pct_change(),
        "Log_return": np.log(sub_data[attribute]) - np.log(sub_data[attribute].shift(1)),
    }

    return stat_dict[stat_method]

