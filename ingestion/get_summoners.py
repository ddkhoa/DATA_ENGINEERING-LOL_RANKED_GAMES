from datetime import datetime, timedelta
from time import sleep
import requests
import json
import os
import random

from common import HOST, X_RIOT_TOKEN, SUMMONERS_SIZE, get_date_label, data_directory, write_csv

divisions = ["I", "II", "III", "IV"]
tiers = ["DIAMOND", "PLATINUM", "GOLD", "SILVER", "BRONZE", "IRON"]
queues = ["RANKED_SOLO_5x5"]

summoner_fieldnames = ["leagueId", "queueType", "tier",  "rank",  "summonerId", "summonerName",
                       "leaguePoints", "wins", "losses", "veteran", "inactive", "freshBlood", "hotStreak", "miniSeries"]

def get_summoners_riot_api(queue, tier, division, page=1):

    endpoint = "/lol/league/v4/entries/{queue}/{tier}/{division}?page={page}".format(
        queue=queue, tier=tier, division=division, page=page)
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
        return get_summoners_riot_api(queue, tier, division, page=1)

    data_text = response.text
    data_json = json.loads(data_text)
    return data_json


def get_summoners(date: datetime):

    if not date:
        date = datetime.today() - timedelta(days=1)
        
    if not os.path.isdir(data_directory):
        os.mkdir(data_directory)

    date_label = get_date_label(date)
    summoners_csv = os.path.join(data_directory, "summoners_{date_label}.csv".format(date_label=date_label))

    summoners_list = []

    for tier in tiers:
        for division in divisions:
            for queue in queues:
                # 1 page, ~200 summoners/page -> about 200 summoners per (division/tier)
                summoners_data = get_summoners_riot_api(
                    division=division, tier=tier, queue=queue, page=1)
                # only get first SUMMONERS_SIZE summoners instead of 200 to reduce datasize
                random.shuffle(summoners_data)
                summoners_list.extend(summoners_data[:SUMMONERS_SIZE])

    write_csv(summoners_list, summoners_csv, 'w', summoner_fieldnames)
               