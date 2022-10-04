import os
import pandas as pd

def data_extract(PATH_DIR, chunksize=10**5, frac=0.01):
    """
    나누어져 있는 csv 파일의 데이터에서 일부를 랜덤하게 추출하고 합칩니다.

    PATH_DIR : csv 파일이 들어 있는 폴더 경로
    chunksize : 데이터를 몇개 단위로 불러올지 (default = 10**5)
    frac : 'chunksize' 개의 데이터에서 몇 퍼센트를 추출할지 (default = 0.01)
    """
 
    file_list = os.listdir(PATH_DIR)
    csv_list = [file for file in file_list if file.endswith('.csv')]

    df = pd.DataFrame(columns=['inDate', 'game_id', 'gamer_id', 'url', 'method', 'tableAndColumn'])

    for csv in csv_list:
        for index, chunk in enumerate(pd.read_csv(PATH_DIR+csv, chunksize=chunksize)):
            df_tmp = chunk.sample(frac=frac, random_state=24)
            df_tmp = df_tmp.sort_values(by=['inDate'], axis=0)
            df = pd.concat([df, df_tmp], axis = 0)
        print(csv, "Success!")

    df.drop(['Unnamed: 0'], axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df