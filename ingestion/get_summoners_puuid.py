from datetime import datetime, timedelta
from time import sleep
import os

from google.cloud import storage
from common import HOST, X_RIOT_TOKEN, get_date_label, data_directory, CLOUD_STORAGE_SIDE_INPUT_DIR, bucket, storage_client, read_csv, write_csv
import requests
import json
import sys


def get_summoner_by_name(summoner_name):
    endpoint = "/lol/summoner/v4/summoners/by-name/{summoner_name}".format(
        summoner_name=summoner_name)
    url = "https://{HOST}{endpoint}".format(HOST=HOST, endpoint=endpoint)
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
        return get_summoner_by_name(summoner_name)

    data_text = response.text
    data_json = json.loads(data_text)
    return data_json

def get_summoners_puuid(date: datetime=None):

    if not date:
        date = datetime.today() - timedelta(days=1)

    date_label = get_date_label(date)

    # input: summoners csv file
    summoners_csv = os.path.join(data_directory, "summoners_{date_label}.csv".format(date_label=date_label))
    summoners_list = read_csv(summoners_csv)

    # side input: puuid cache
    puuid_cache = {}
    puuid_cache_path =  "{}/puuid_cache.json".format(CLOUD_STORAGE_SIDE_INPUT_DIR)
    puuid_cache_blob = bucket.blob(puuid_cache_path)
    is_puuid_cache_exists = storage.Blob(name=puuid_cache_path, bucket=bucket).exists(storage_client)

    if is_puuid_cache_exists:
        puuid_cache = json.loads(puuid_cache_blob.download_as_string(client=None))

    for item in summoners_list:
        name = item['summonerName']

        if name in puuid_cache:
            print("Cache hit")
            item['puuid'] = puuid_cache[name]
            continue

        try :
            summoner_data = get_summoner_by_name(name)

            # if 'status' in summoner_data and summoner_data['status']['status_code'] == 429:
            #     print("Hit rate limit. Sleep 2 minutes")
            #     sleep(60.0 * 2)
            #     print("Continue")
            #     summoner_data = get_summoner_by_name(name)
            
            if 'status' in summoner_data and summoner_data['status']['status_code'] == 404:
                print(name, " not found")
                item['puuid'] = None
                continue

            puuid = summoner_data['puuid']
            item['puuid'] = puuid
            puuid_cache[name] = puuid
        except:
            print(sys.exc_info()[0], sys.exc_info()[1])
            print(name, summoner_data)
            item['puuid'] = None
            continue

    # print(summoners_list)
    summoners_list = [item for item in summoners_list if item['puuid']]
    summoner_puuid_fieldnames = ["leagueId", "queueType", "tier",  "rank",  "summonerId", "summonerName",
                       "leaguePoints", "wins", "losses", "veteran", "inactive", "freshBlood", "hotStreak", "miniSeries", "puuid"]
    write_csv(summoners_list, summoners_csv, 'w', summoner_puuid_fieldnames)

    # ensure_ascii = False because some summoners name have accent
    puuid_cache_serialized = json.dumps(puuid_cache, indent = 4, ensure_ascii=False)
    puuid_cache_blob.upload_from_string(data=puuid_cache_serialized, content_type='application/json')
        

