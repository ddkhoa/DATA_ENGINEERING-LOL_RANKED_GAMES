import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

def read_glob_csv_to_df(glob_files):
    li = []

    for filename in glob_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)

    return df

def convert_columns_to_type(df: pd.DataFrame, columns: list[str], type):

    for column in columns:
        df[column] = df[column].astype(type)

    return df

data_directory = os.path.join(os.path.dirname(__file__), "..", "ingestion", "data")
match_files = glob.glob(os.path.join(data_directory, "matches_data_*.csv"))
matches_df = read_glob_csv_to_df(match_files)

print(
    "Before optimizing, matches data consumes {} mb in memory".format(
        matches_df.memory_usage(deep=True).sum() // (1024 * 1024)
    )
)

matches_df = matches_df.dropna()
matches_df['tier'] = pd.Categorical(matches_df['tier'], 
                      categories=["IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "DIAMOND"],
                      ordered=True)
matches_df = convert_columns_to_type(matches_df, ["gameVersion"], "category")
matches_df = convert_columns_to_type(
    matches_df,
    [
        "200_firstBaron",
        "200_firstKill",
        "200_firstDragon",
        "200_firstRiftHerald",
        "200_firstTower",
        "200_win",
    ],
    "bool",
)
matches_df = convert_columns_to_type(
    matches_df,
    [
        "100_nbBarons",
        "200_nbBarons",
        "100_nbDragons",
        "200_nbDragons",
        "100_nbRiftHeralds",
        "200_nbRiftHeralds",
        "100_nbTowers",
        "200_nbTowers",
    ],
    "int8",
)

matches_df = convert_columns_to_type(
    matches_df, ["100_nbKills", "200_nbKills"], "int16"
)  # to make sure no overflow
matches_df.drop(
    axis=1, labels=["gameStartTimestamp", "gameEndTimestamp", "mapId"], inplace=True
)

print(
    "After optimizing, matches data consumes {} mb in memory".format(
        matches_df.memory_usage(deep=True).sum() // (1024 * 1024)
    )
)

duration_by_rank = matches_df.pivot_table(index=['matchId'], columns='tier', values='gameDuration')

# duration_by_rank.plot.box()
# plt.show()
# => The higher the tier, more consistency the game


winrate_by_rank = matches_df.groupby(['tier']).agg({
    '100_win': 'sum',
    '200_win': 'sum',
    'matchId': 'count'
}).reset_index()

winrate_by_rank.rename(columns={'matchId': 'total', '100_win': 'blue_win', '200_win': 'red_win'}, inplace=True)
winrate_by_rank['blue_winrate'] = winrate_by_rank['blue_win'] * 100.0 / winrate_by_rank['total']
winrate_by_rank['red_winrate'] = winrate_by_rank['red_win'] * 100.0 / winrate_by_rank['total']

# plt.bar(winrate_by_rank['tier'], winrate_by_rank['blue_winrate'], color='b')
# plt.bar(winrate_by_rank['tier'], winrate_by_rank['red_winrate'], bottom=winrate_by_rank['blue_winrate'], color='r')
# plt.show()
# => The winrate doesn't depend on the tier and mostly equal for 2 sides. There aren't advantages for neither blue or red side.


matches_df['total_barons'] = matches_df['100_nbBarons'] + matches_df['200_nbBarons'] 
matches_df['total_dragons'] = matches_df['100_nbDragons'] + matches_df['200_nbDragons'] 
matches_df['total_towers'] = matches_df['100_nbTowers'] + matches_df['200_nbTowers'] 
matches_df['total_heralds'] = matches_df['100_nbRiftHeralds'] + matches_df['200_nbRiftHeralds'] 
matches_df['total_kills'] = matches_df['100_nbKills'] + matches_df['200_nbKills'] 

objectives_by_rank = matches_df.groupby(['tier']).agg({
    'total_barons': 'mean',
    'total_dragons': 'mean',
    'total_towers': 'mean',
    'total_heralds': 'mean',
    'total_kills': 'mean',
}).reset_index()


# Initialise the subplot function using number of rows and columns
figure, axis = plt.subplots(3, 2)

# axis[0, 0].plot(objectives_by_rank['tier'], objectives_by_rank['total_barons'], color='b')
# axis[0, 0].set_title("Average barons per games")
# axis[0, 1].plot(objectives_by_rank['tier'], objectives_by_rank['total_dragons'], color='b')
# axis[0, 1].set_title("Average dragons per games")
# axis[1, 0].plot(objectives_by_rank['tier'], objectives_by_rank['total_towers'], color='b')
# axis[1, 0].set_title("Average towers per games")
# axis[1, 1].plot(objectives_by_rank['tier'], objectives_by_rank['total_heralds'], color='b')
# axis[1, 1].set_title("Average heralds per games")
# axis[2, 0].plot(objectives_by_rank['tier'], objectives_by_rank['total_kills'], color='b')
# axis[2, 0].set_title("Average kills per games")
# plt.show()
# In lower tier, there are more kills. In contrast, players in higher tier priotize objectives like heralds, towers or barons to win the game.
# There are fewer dragons taken in high tier (PLATINUM, DIAMOND). This can be relate to the game duration. Games in these tier tend to finish quickly,
# so the number of dragons taken is low


champ_files = glob.glob(os.path.join(data_directory, "champs_data_*.csv"))
champs_df = read_glob_csv_to_df(champ_files)
# print(champs_df.head())
print(
    "Before optimizing, champs data consumes {} mb in memory".format(
        champs_df.memory_usage(deep=True).sum() // (1024 * 1024)
    )
)

champs_df['tier'] = pd.Categorical(champs_df['tier'], 
                      categories=["IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "DIAMOND"],
                      ordered=True)
champs_df = convert_columns_to_type(champs_df, ['teamPosition', 'teamId'], 'category')

# For ban champions, assists, deaths, kills is Null
champs_df['assists'].fillna(0, inplace=True)
champs_df['deaths'].fillna(0, inplace=True)
champs_df['kills'].fillna(0, inplace=True)
champs_df = convert_columns_to_type(champs_df, ['assists', 'deaths', 'kills'], 'int16')

# For pick champions, turn (ban turn) is Null
champs_df['turn'].fillna(0, inplace=True)
champs_df = convert_columns_to_type(champs_df, ['turn'], 'int8')

champs_df['gameStartTime'] = pd.to_datetime(champs_df['gameStartTime'])
champs_df.drop(axis=1,labels=['championId'], inplace=True)

# print(champs_df.dtypes)
print(
    "After optimizing, champs data consumes {} mb in memory".format(
        champs_df.memory_usage(deep=True).sum() // (1024 * 1024)
    )
)

# calcul stats per tier/position/champion
champs_stats = champs_df.groupby(['tier', 'teamPosition', 'championName']).agg({
    'matchId': 'count',
    'win': 'sum',
    'assists': 'mean',
    'deaths': 'mean',
    'kills': 'mean',
    'pick' : 'sum',
}).reset_index()
# print(champs_stats.head(50))

# for each tier/position, find the most picked champion
most_picked_champs = champs_stats.groupby(['tier', 'teamPosition']).apply(lambda x: x.sort_values(['pick'], ascending=False)).reset_index(drop=True)
print(most_picked_champs.groupby(['tier', 'teamPosition']).head(3))

# win
high_winrate_champs = champs_stats.groupby(['tier', 'teamPosition']).apply(lambda x: x.sort_values(['win'], ascending=False)).reset_index(drop=True)
print(high_winrate_champs.groupby(['tier', 'teamPosition']).head(3))

# calculate KDA = (K + A)/D
champs_stats['KDA'] = (champs_stats['kills'] + champs_stats['assists'])/champs_stats['deaths']
high_KDA_champs = champs_stats.groupby(['tier', 'teamPosition']).apply(lambda x: x.sort_values(['KDA'], ascending=False)).reset_index(drop=True)
print(high_KDA_champs.groupby(['tier', 'teamPosition']).head(3))


# ban does not take into account the position
most_banned_champs = champs_df.groupby(['tier', 'championName']).agg({
    'ban': 'sum'
}).reset_index()

most_banned_champs = most_banned_champs.groupby(['tier']).apply(lambda x: x.sort_values(['ban'], ascending=False)).reset_index(drop=True)
print(most_banned_champs.groupby(['tier']).head(3))

# MasterYi, Pyke, Yasuo and Zed gets most bans in lower tier. Players in higher tier know how to counter this champs so they don't ban him.
# Yummi gets a lot of bans in higher tier.
