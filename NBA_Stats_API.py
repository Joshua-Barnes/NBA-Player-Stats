import requests
import json
import pandas as pd
import numpy as np
import psycopg2

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

################################################################################################################################
def get_team_abbr():
    # Create a dataframe that will hold the abbreviations of all of the NBA teams
    team_abbreviations = pd.DataFrame({'Abbr': ['ATL','BKN','BOS','CHA','CHI','CLE','DAL','DEN','DET','GS','HOU','IND','LAC',
                                      'LAL','MEM','MIA','MIL','MIN','NO','NY','OKC','ORL','PHI','PHO','POR','SAC','SA','TOR','UTA','WAS'],

                             'City': ['Atlanta Hawks','Brooklyn Nets','Boston Celtics','Charlotte Hornets','Chicago Bulls',
                                      'Cleveland Cavaliers','Dallas Mavericks','Denver Nuggets','Detroit Pistons', 'Golden State Warriors',
                                      'Houston Rockets','Indiana Pacers','Los Angeles Clippers','Los Angeles Lakers','Memphis Grizzlies',
                                      'Miami Heat','Milwaukee Bucks','Minnesota Timberwolves','New Orleans Pelicans','New York Knicks','Oklahoma City Thunder',
                                      'Orlando Magic', 'Philadelphia 76ers','Phoenix Suns','Portland Trail Blazers','Sacramento Kings','San Antonio Spurs',
                                      'Toronto Raptors','Utah Jazz','Washington Wizards']})
    #print(team_abbreviations, '\n\n')

    return team_abbreviations
get_team_abbr()
################################################################################################################################

def get_player_stats():
    #---------------------------------------------------
    #json_data = call_api()
    # Print the entire JSON file "pretty"
    #print(json.dumps(json_data, indent=4, sort_keys=True))
    #-----------------------------------------------------

    # Get the list of team abbreviations
    team_abbreviations = get_team_abbr()

    # api key: a4cf373d1e984cba9b716c8895a82699
    # Get the api response for the player stats by team
    # Base URL: "https://api.sportsdata.io/v3/nba/stats/json/Players/INSERT_CITY_ABBR?key=a4cf373d1e984cba9b716c8895a82699"
    base_url = "https://api.sportsdata.io/v3/nba/stats/json/Players/xxx?key=a4cf373d1e984cba9b716c8895a82699"


    append_dataframe = pd.DataFrame(columns=['PlayerID', 'FirstName', 'LastName', 'Status', 'TeamID', 'Team', 'Position',
                                                'Height', 'Weight', 'BirthDate', 'BirthCity', 'BirthState', 'College',
                                                'Salary'])

    #print(append_dataframe)

    # Create a counter that will help iterate through the dataframe that contains the city abbreviations so that it will be inserted
        # into the url
    counter = 0

    # Create a loop that will insert the correct team abreviation into the api url and print out the result for each team
    for x in team_abbreviations.iterrows():
        # Replace 'xxx' with the team abbreviation
        url_insert_team = base_url.replace("xxx", team_abbreviations.loc[counter][0])
        response = requests.request("GET", url_insert_team)

        # Parse the JSON file
        json_data = json.loads(response.text)

        # Turn the entire json file into a dataframe
        json_df_all_columns = pd.json_normalize(json_data)
        #print(json_df_all_columns)

        # Get the columns of interest from the larger dataframe
        json_df_selected_columns = json_df_all_columns[['PlayerID', 'FirstName', 'LastName', 'Status', 'TeamID', 'Team',
                                                        'Position', 'Height', 'Weight', 'BirthDate', 'BirthCity',
                                                        'BirthState', 'College', 'Salary']].copy()

        # Increase the counter by 1 so that the next team can be called
        counter = counter + 1
        # Append the api response for each individual team into a dataframe
        append_dataframe = append_dataframe.append(json_df_selected_columns)

        # Print out the api response for each team individually
        #print(json_df_selected_columns,'\n--------------------\n')

    # Print the dataframe with the specific columns
    #print(json_df_selected_columns)

    # Adjust the indexing of the dataframe
    append_dataframe.index = np.arange(0, len(append_dataframe))

    # Print the dataframe that contains all of the team data
    print('\n', append_dataframe)

    # Return append_dataframe so that it can be accessed by future functions
    return append_dataframe

get_player_stats()
################################################################################################################################

'''
def write_to_database():
    append_dataframe = get_player_stats()
    print('\n', append_dataframe)

    # Create database connection
    connection = psycopg2.connect(host="localhost", database="NBA_Player_Info", user="postgres", password="onethrough8")

    # Create a cursor so to execute queries
    cursor = connection.cursor()

    # Print PostgreSQL Connection properties
    print('\nDatabase Connection info: ', connection.get_dsn_parameters(), "\n", sep="")
    print('Successfully connected to database!')


write_to_database()
'''