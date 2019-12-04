import pandas as pd
import seaborn as sns

# data paths
PBP_PATH = 'https://raw.githubusercontent.com/ryurko/nflscrapR-data/master/play_by_play_data/regular_season/reg_pbp_2019.csv'
PLAYER_PATH = 'https://raw.githubusercontent.com/ryurko/nflscrapR-data/master/roster_data/regular_season/reg_roster_2019.csv'
GAME_PATH = 'https://raw.githubusercontent.com/ryurko/nflscrapR-data/master/games_data/regular_season/reg_games_2019.csv'

# load the data
pbp = pd.read_csv(PBP_PATH, usecols=['play_id', 'game_id', 'game_date',
                                     'posteam', 'defteam', 'play_type',
                                     'complete_pass', 'receiver_player_id',
                                     'receiver_player_name',
                                     'rusher_player_id', 'rusher_player_name'])
player = pd.read_csv(PLAYER_PATH)
games = pd.read_csv(GAME_PATH)

# look at it (run these one at a time)
pbp.head()
player.head()
games.head()

# market share is:
# WR/TE: targets divided by team passing attempts
# RB: touches divided by team plays from scrimmage

# so need touches, targets per player and plays, passing plays per team

# get number of rushes per game/player
weekly_rush_stats = (pbp
                     .query("play_type == 'run'")
                     .groupby(['game_id', 'rusher_player_id', 'rusher_player_name'])
                     .agg(carries = ('play_id', 'count'))
                     .reset_index()
                     .rename(columns={
                         'rusher_player_id': 'player_id',
                         'rusher_player_name': 'player_name'}))

# get number of targets and completions per game/player
weekly_rec_stats = (pbp
                    .query("play_type == 'pass'")
                    .groupby(['game_id', 'receiver_player_id', 'receiver_player_name'])
                    .agg(targets = ('play_id', 'count'),
                         catches = ('complete_pass', 'sum'))
                    .reset_index()
                    .rename(columns={
                        'receiver_player_id': 'player_id',
                        'receiver_player_name': 'player_name'}))

# combine rushes and targets
weekly_stats_player = pd.merge(weekly_rec_stats, weekly_rush_stats, how='outer').fillna(0)

# calculate touches
weekly_stats_player['touches'] = weekly_stats_player['catches'] + weekly_stats_player['carries']

# now get total number of offensive plays (pass or run) per team and game
team_total_plays = (pbp.query("(play_type == 'pass') | (play_type == 'run')")
                         .groupby(['game_id', 'posteam'])
                         .agg(total_plays = ('play_id', 'count'))
                         .reset_index()
                         .rename(columns={'posteam': 'team'}))


# now get total number of just passing plays per team and game
team_passing_attempts = (pbp.query("play_type == 'pass'")
                         .groupby(['game_id', 'posteam'])
                         .agg(team_pass_attempts = ('play_id', 'count'))
                         .reset_index()
                         .rename(columns={'posteam': 'team'}))

# stick n of plays and passes together
weekly_stats_team = pd.merge(team_passing_attempts, team_total_plays)

# now we're almost ready to combine game/player and game/team data

# but first need to add in team to the game/player table
weekly_stats_player = pd.merge(
    weekly_stats_player,
    player[['gsis_id', 'team', 'position']],
    how='left', left_on='player_id', right_on='gsis_id')

# also add week number while we're at it
weekly_stats_player = pd.merge(weekly_stats_player, games[['game_id', 'season', 'week']])

# now finally let's combine game/team and game/player data
weekly_stats = pd.merge(weekly_stats_player, weekly_stats_team)

# now that we have that we can calculate market share
weekly_stats['rb_market_share'] = weekly_stats['touches']/weekly_stats['total_plays']
weekly_stats['rec_market_share'] = weekly_stats['targets']/weekly_stats['team_pass_attempts']


# look at the results
rec_market_share_wk12 = (
    weekly_stats.query("(week == 12) & ((position == 'WR') | (position == 'TE'))")
    [['player_name', 'position', 'rec_market_share']]
    .sort_values('rec_market_share', ascending=False))

rb_market_share_wk12 = (
    weekly_stats.query("(week == 12) & (position == 'RB')")
    [['player_name', 'rb_market_share']].sort_values('rb_market_share',
                                                     ascending=False))

rb_market_share_all = (
    weekly_stats.query("(position == 'RB')")[['player_name', 'week', 'rb_market_share']]
    .sort_values('rb_market_share', ascending=False))

rec_market_share_all = (
    weekly_stats.query("(position == 'WR') | (position == 'TE')")
    [['player_name','position',  'week', 'rec_market_share']]
    .sort_values('rec_market_share', ascending=False))

# output them to csv
rec_market_share_all.to_csv('rec_market_share_all.csv', index=False)
rb_market_share_all.to_csv('rb_market_share_all.csv', index=False)

# plotting section

# look at average recieving market share over time
# main plot
g = sns.relplot(x='week', y='rec_market_share', kind='line', aspect=1.2,
                data=rec_market_share_all)
# add title
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle('Recieving Market Share By Week')

g.savefig('rec_market_share_by_week.png') # save

# look at average market share over time by position
g = sns.relplot(x='week', y='rec_market_share', kind='line', hue='position',
                aspect=1.2, data=rec_market_share_all)

g.fig.subplots_adjust(top=0.9)
g.fig.suptitle('Recieving Market Share By Position and Week')

g.savefig('rec_market_share_by_pos_week.png') # save

# look at max (for each position) market share over time
g = sns.relplot(x='week', y='rec_market_share', kind='line', hue='position',
                aspect=1.2,
                data=rec_market_share_all
                .groupby(['week', 'position'])
                .max()
                .reset_index())
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle('Maximum Recieving Market Share By Position and Week')
g.savefig('max_rec_market_share_by_pos_week.png') # save

# look at average market share over time of the top 20 rbs
# first need to identify the top 20 rbs
top_rb = (rb_market_share_all
          .groupby('player_name')
          .agg(ave_market_share = ('rb_market_share', 'mean'))
          .sort_values('ave_market_share', ascending=False)
          .head(20)
          .reset_index())

# then plot
g = sns.relplot(x='week', y='rb_market_share', kind='line', col='player_name',
                hue='player_name', col_wrap=4, height=2, aspect=1.2,
                data=pd.merge(rb_market_share_all, top_rb), legend=False)
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle('Market Share By Week - Top 20 RBs')
g.savefig('top20_rb_market_share_by_week.png') # save
