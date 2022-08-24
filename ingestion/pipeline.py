from datetime import datetime, timedelta
import time
import argparse

from dependencies.prepare import prepare
from dependencies.get_summoners import get_summoners
from dependencies.get_summoners_puuid import get_summoners_puuid
from dependencies.get_matches_id import get_matches_id
from dependencies.get_match_data import get_match_data


def parse_string(str):
    """
    It takes a string in the format YYYYMMDD and returns a datetime object
    
    :param str: The string to parse
    :return: A datetime object
    """
    year = int(str[:4])
    month = int(str[4:6])
    day = int(str[6:])

    return datetime(year, month, day)

def run_pipeline(date_arg):
    """
    `run_pipeline` is a function that takes a date as an argument and runs the pipeline for that date
    
    :param date_arg: The date you want to get data for. If you don't specify a date, it will get data
    for the previous day
    """

    if date_arg:
        date = parse_string(date_arg)
    else:
        date = datetime.today() - timedelta(days=1)

    print("Start get data for {}".format(date))
    start_all = time.time()

    prepare()
    end_prepare = time.time()
    print("Finish prepare")
    
    get_summoners(date)
    end_get_summoners = time.time()
    print("Finish get summoners")

    get_summoners_puuid(date)
    end_get_puuid = time.time()
    print("Finish get summoners puuid")
    
    get_matches_id(date)
    end_get_matches_id = time.time()
    print("Finish get matches id")

    get_match_data(date)
    end_all = time.time()
    print("Finish all")

    print('Prepare time:', end_prepare - start_all, 'seconds')
    print('Get summoners time:', end_get_summoners - end_prepare, 'seconds')
    print('Get puuid time:', end_get_puuid - end_get_summoners, 'seconds')
    print('Get match id time:', end_get_matches_id - end_get_puuid, 'seconds')
    print('Get match data time:', end_all - end_get_matches_id, 'seconds')
    print('Execution time:', end_all - start_all, 'seconds')

def main():
    """
    It takes a date argument, and if it's not provided, it uses the current date and run the pipeline
    """

    parser = argparse.ArgumentParser(description='Get LOL ranked games')
    parser.add_argument("--date", type=str, required=False)

    args = parser.parse_args()
    date_arg = args.date

    run_pipeline(date_arg)

if __name__ == "__main__":
    main()