from time import sleep
import os
from datetime import datetime, timedelta

from dependencies.common import HOST_EU, X_RIOT_TOKEN, get_date_label, data_directory, read_csv, write_csv, CLOUD_STORAGE_DATA_TEMP, bucket
import requests
import json
import sys

def get_matches_by_puuid(puuid, start_date, end_date):
    """
    It takes a puuid, start date and end date and returns a list of match ids
    
    :param puuid: the player's unique ID
    :param start_date: The start date of the time frame you want to get matches from
    :param end_date: The end date of the range of matches to retrieve
    :return: A list of match ids
    """
    endpoint = "/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start_date}&endTime={end_date}&start=0&count=100".format(
        puuid=puuid, start_date=start_date, end_date=end_date)
    url = "https://{HOST}{endpoint}".format(HOST=HOST_EU, endpoint=endpoint)
    headers = {
        "X-Riot-Token": X_RIOT_TOKEN
    }

    response = requests.get(url=url, headers=headers)
    response_headers = response.headers
    
    if 'Retry-After' in response_headers:
        time_sleep = response_headers['Retry-After']
        print("Hit rate limit. Sleep {} seconds".format(time_sleep))
        sleep(int(time_sleep))
        print("Continue")
        return get_matches_by_puuid(puuid, start_date, end_date)
    
    data_text = response.text
    data_json = json.loads(data_text)
    return data_json

def get_start_and_end_timestamp(date: datetime):
    """
    It takes a date as input and returns a dictionary with two keys: 'start' and 'end'. The values of
    these keys are the start and end timestamps of the day
    
    :param date: The date for which you want to get the start and end timestamps
    :type date: datetime
    :return: A dictionary with two keys, start and end.
    """
    year = date.year
    month = date.month
    day = date.day
    start_epoch = datetime(year, month, day, 0, 0, 0).timestamp()
    end_epoch = start_epoch + 86400
    # print(start_epoch, end_epoch)
    result = {
        'start' : str(int(start_epoch)),
        'end': str(int(end_epoch))
    }
    return result

def get_matches_id(date: datetime=None, **kwargs):
    """
    > Get the list of matches played by summoners in a given date range, and save the list of matches to
    a CSV file
    
    :param date: datetime=None, **kwargs
    :type date: datetime
    """

    if not date:
        date = kwargs['execution_date']

    date_label = get_date_label(date)

    summoners_csv_gcs = "{}/summoners_{}.csv".format(CLOUD_STORAGE_DATA_TEMP, date_label)
    summoners_blob = bucket.blob(summoners_csv_gcs)

    summoners_csv_local = os.path.join(data_directory, "summoners_{date_label}.csv".format(date_label=date_label))
    summoners_blob.download_to_filename(summoners_csv_local)
    
    summoners_data = read_csv(summoners_csv_local)

    date_range = get_start_and_end_timestamp(date)
    start_date = date_range['start']
    end_date = date_range['end']

    tier_matches_list = []
    matches_id_list = []

    for item in summoners_data:
        puuid = item['puuid']
        tier = item['tier']
        # print(puuid, tier, item['summonerName'])
        try :
            match_data = get_matches_by_puuid(puuid, start_date, end_date)
            # if 'status' in match_data and match_data['status']['status_code'] == 429:
            #     print("Hit rate limit. Sleep 2 minutes")
            #     sleep(60.0 * 2)
            #     print("Continue")
            #     match_data = get_matches_by_puuid(puuid, start_date, end_date)
            # print(match_data)
            for match_id in match_data:
                if match_id not in matches_id_list:
                    tier_matches_list.append({
                        'tier': tier, 'match_id': match_id
                    })
                    matches_id_list.append(match_id)
                    
        except:
            print(sys.exc_info()[0], sys.exc_info()[1])
            print(match_data)

    # print(tier_matches_list, matches_id_list)
    tier_matches_id_csv_local = os.path.join(data_directory, "matches_id_{date_label}.csv".format(date_label=date_label))
    write_csv(tier_matches_list, tier_matches_id_csv_local, 'w', keys=['tier', 'match_id'])

    tier_matches_id_csv_gcs = "{}/matches_id_{}.csv".format(CLOUD_STORAGE_DATA_TEMP, date_label)
    tier_matches_id_blob = bucket.blob(tier_matches_id_csv_gcs)

    tier_matches_id_blob.upload_from_filename(tier_matches_id_csv_local)


