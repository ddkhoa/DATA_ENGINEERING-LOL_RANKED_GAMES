# create champs lookup and put on gcs

import requests, json
from dependencies.common import CLOUD_STORAGE_SIDE_INPUT_DIR, bucket

def prepare():
    """
    It downloads the latest champion data from Riot's API, converts the champion name to champion id,
    and uploads the result to a file in Cloud Storage
    """
    champs_data_url="http://ddragon.leagueoflegends.com/cdn/12.10.1/data/en_US/champion.json"
    response = requests.get(champs_data_url)
    champs_data = response.json()

    # convert Name - Id to Id - Name
    champs_data = champs_data['data']
    id_name_map = {}

    for champ in champs_data:
        champ_id = champs_data[champ]['key']
        id_name_map[champ_id] = champ
    
    id_name_map_serialized = json.dumps(id_name_map, indent = 4, ensure_ascii=False)
    champs_lookup_path =  "{}/champs_lookup.json".format(CLOUD_STORAGE_SIDE_INPUT_DIR)
    champs_lookup_blob = bucket.blob(champs_lookup_path)
    champs_lookup_blob.upload_from_string(data=id_name_map_serialized, content_type='application/json')
