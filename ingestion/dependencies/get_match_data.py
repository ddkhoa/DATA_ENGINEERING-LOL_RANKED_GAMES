from datetime import datetime, timedelta
import sys
import os
import requests
import json
from time import sleep

from dependencies.common import HOST_EU, X_RIOT_TOKEN, get_date_label, data_directory, CLOUD_STORAGE_DATA_TEMP, CLOUD_STORAGE_SIDE_INPUT_DIR, CLOUD_STORAGE_DATA_DIR, bucket, read_csv, write_csv, load_csv_to_bigquery, CHAMPIONS_TABLE, MATCHES_TABLE

def get_match_data_by_id(id):
    endpoint = "/lol/match/v5/matches/{id}/".format(
        id=id)
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
        return get_match_data_by_id(id)

    data_text = response.text
    data_json = json.loads(data_text)
    return data_json

def filter_attributes_match_obj(match_obj, tier, champs_lookup):
    
    match_stats = {}
    champ_list = []

    # get match stats
    game_start_time = datetime.fromtimestamp(match_obj['info']['gameStartTimestamp'] // 1000)
    game_end_time = datetime.fromtimestamp(match_obj['info']['gameEndTimestamp'] // 1000)

    match_id= match_obj['metadata']['matchId']
    match_stats['matchId'] = match_id
    match_stats['tier'] = tier
    match_stats['gameStartTimestamp'] = match_obj['info']['gameStartTimestamp']
    match_stats['gameEndTimestamp'] = match_obj['info']['gameEndTimestamp']
    match_stats['gameStartTime'] = game_start_time
    match_stats['gameEndTime'] = game_end_time
    match_stats['gameDuration'] = match_obj['info']['gameDuration']
    match_stats['mapId'] = match_obj['info']['mapId']
    match_stats['gameVersion'] = '.'.join(match_obj['info']['gameVersion'].split('.')[:2])

    for team in match_obj['info']['teams']:
        teamId = team['teamId']
        objectives = team['objectives']

        match_stats[str(teamId) + '_firstBaron'] = objectives['baron']['first']
        match_stats[str(teamId) + '_nbBarons'] = objectives['baron']['kills']
        match_stats[str(teamId) + '_firstKill'] = objectives['champion']['first']
        match_stats[str(teamId) + '_nbKills'] = objectives['champion']['kills']
        match_stats[str(teamId) + '_firstDragon'] = objectives['dragon']['first']
        match_stats[str(teamId) + '_nbDragons'] = objectives['dragon']['kills']
        match_stats[str(teamId) + '_firstRiftHerald'] = objectives['riftHerald']['first']
        match_stats[str(teamId) + '_nbRiftHeralds'] = objectives['riftHerald']['kills']
        match_stats[str(teamId) + '_firstTower'] = objectives['tower']['first']
        match_stats[str(teamId) + '_nbTowers'] = objectives['tower']['kills']
        match_stats[str(teamId) + '_win'] = team['win']

    # get data about champions:  KDA of picked champs and ban turn for banned champs
    champs_attributes = ['assists', 'deaths', 'kills', 'championId', 'teamPosition', 'teamId']

    for participant in match_obj['info']['participants']:

        pick_champ = {'matchId': match_id, 'gameStartTime': game_start_time, 'tier': tier, 'pick': True, 'ban' : False}
        for attribute in champs_attributes:
            pick_champ[attribute] = participant[attribute]

        for team in match_obj['info']['teams']:
            teamId = team['teamId']
            if teamId == pick_champ['teamId']:
                pick_champ['win'] = team['win']
            else:
                pick_champ['win'] = not team['win']
            break
            
        pick_champ['championName'] = champs_lookup[str(pick_champ['championId'])]

        opponent = filter(lambda p: p['teamPosition'] == pick_champ['teamPosition'] and p['teamId'] != pick_champ['teamId'],
                                     match_obj['info']['participants'])
        opponent = list(opponent)
        if len(opponent) > 0:
            pick_champ['opponent'] = champs_lookup[str(opponent[0]['championId'])]
        else:
            pick_champ['opponent'] = None
        
        champ_list.append(pick_champ)


    for team in match_obj['info']['teams']:

        teamId = team['teamId']
        for ban in team['bans']:
            ban_champ = {'matchId': match_id, 'gameStartTime': game_start_time, 'tier': tier, 'pick': False, 'ban': True, 'turn': ban['pickTurn'], 'championId': ban['championId'], 'teamId' : teamId }
            
            if ban_champ['championId'] == -1 :
                ban_champ['championName'] = None
            else:
                ban_champ['championName'] = champs_lookup[str(ban_champ['championId'])]
            
            ban_champ['win'] = team['win']

            champ_list.append(ban_champ)
 
    result = {
        'match_stats': match_stats, 
        'champ_list': champ_list
    }
    return result

def get_match_data(date: datetime=None, **kwargs):

    if not date:
        date = kwargs['execution_date']

    date_label = get_date_label(date)

    # read input
    matches_id_csv_gcs = "{}/matches_id_{}.csv".format(CLOUD_STORAGE_DATA_TEMP, date_label)
    matches_id_blob = bucket.blob(matches_id_csv_gcs)

    matches_id_csv_local = os.path.join(data_directory, "matches_id_{date_label}.csv".format(date_label=date_label))
    matches_id_blob.download_to_filename(matches_id_csv_local)
    
    match_id_list = read_csv(matches_id_csv_local)

    # side input
    champs_lookup_path =  "{}/champs_lookup.json".format(CLOUD_STORAGE_SIDE_INPUT_DIR)
    champs_lookup_blob = bucket.blob(champs_lookup_path)
    champs_lookup = json.loads(champs_lookup_blob.download_as_string(client=None))

    matches = []
    champs = []
    processed = 0
    nb_to_process = len(match_id_list)

    for item in match_id_list:

        # processed = processed + 1
        # print("Process {} / {}".format(processed, nb_to_process))

        id = item['match_id']
        tier = item['tier']

        try:
            match_data = get_match_data_by_id(id)
            # if 'status' in match_data and match_data['status']['status_code'] == 429:
            #     print("Hit rate limit. Sleep 2 minutes")
            #     sleep(60.0 * 2)
            #     print("Continue")
            #     match_data = get_match_data_by_id(id)
            while 'status' in match_data and match_data['status']['status_code'] == 503:
                sleep(1.0 * 2)
                match_data = get_match_data_by_id(id)


            if match_data['info']['gameMode'] != "CLASSIC" and match_data['info']['mapId'] != 11:
                # print(match_data['info']['gameMode'], match_data['info']['mapId'])
                continue

            match_data_transformed = filter_attributes_match_obj(match_data, tier, champs_lookup)

            matches.append(match_data_transformed['match_stats'])
            champs.extend(match_data_transformed['champ_list'])
            
        except:
            print(sys.exc_info()[0], sys.exc_info()[1])
            continue

    # output
    matches_data_csv = os.path.join(data_directory, "matches_data_{date_label}.csv".format(date_label=date_label))
    champs_data_csv = os.path.join(data_directory, "champs_data_{date_label}.csv".format(date_label=date_label))

    # writeto csv
    matches_fieldnames = [
        "matchId","tier","gameStartTimestamp","gameEndTimestamp","gameStartTime","gameEndTime","gameDuration","mapId","gameVersion",
        "100_firstBaron","100_nbBarons","100_firstKill","100_nbKills","100_firstDragon","100_nbDragons","100_firstRiftHerald",
        "100_nbRiftHeralds","100_firstTower","100_nbTowers","100_win","200_firstBaron","200_nbBarons","200_firstKill","200_nbKills",
        "200_firstDragon","200_nbDragons","200_firstRiftHerald","200_nbRiftHeralds","200_firstTower","200_nbTowers","200_win"
    ]
    write_csv(matches, matches_data_csv, 'w', matches_fieldnames)

    champions_fieldnames = ["matchId","gameStartTime","tier","pick","ban","win","assists","deaths","kills",
                                "championId","championName","opponent","teamPosition","teamId","turn"]
    write_csv(champs, champs_data_csv, 'w', champions_fieldnames)

    # upload to gcs
    blob_matches = bucket.blob("{}/matches_data_{}.csv".format(CLOUD_STORAGE_DATA_DIR, date_label))
    blob_matches.upload_from_filename(matches_data_csv)
    blob_champs = bucket.blob("{}/champs_data_{}.csv".format(CLOUD_STORAGE_DATA_DIR, date_label))
    blob_champs.upload_from_filename(champs_data_csv)

    # batch load csv to bigquery (free!)
    load_csv_to_bigquery(file_path=champs_data_csv, table_id=CHAMPIONS_TABLE)
    load_csv_to_bigquery(file_path=matches_data_csv, table_id=MATCHES_TABLE)