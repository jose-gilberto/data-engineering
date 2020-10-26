import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Processa os dados que estão nos arquivos de músicas e insere eles no banco Postgres.
    :param cur: referência para o cursor.
    :param filepath: caminho completo para o arquivo.
    """
    
    # abre o arquivo de música
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])
    
    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value
        
        # insere o registro de artista
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)
        
        # insere o registro da música
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
    
    print(f"Records inserted for file {filepath}")
    
    
def process_log_file(cur, filepath):
    """
    Processa os dados do arquivo de log e insere eles na base Postgres.
    :param cur: referência para o cursor.
    :param filepath: caminho completo para o arquivo.
    """
    # abre o arquivo de log
    df = df = pd.read_json(filepath, lines=True)
    
    # filtra pelas ações de 'NextSong'
    df = df[df['page'] == 'NextSong'].astype({'ts': 'datetime64[ms]'})
    
    # converte a coluna timestamp para datetime
    t = pd.Series(df['ts'], index=df.index)
    
    # insere os dados de tempo
    column_labels = ["timestamp", "hour", "day", "weelofyear", "month", "year", "weekday"]
    time_data = []
    
    for data in t:
        time_data.append([data ,data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    # carrega a tabela de usuários
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # insere os dados de usuários
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insere os dados da tabela de songplays
    for index, row in df.iterrows():
        
        # busca o songid e artistid das tabelas de artistas e músicas
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insere o registro na tabela
        songplay_data = ( row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)  


def process_data(cur, conn, filepath, func):
    """
    Função que carrega os dados de arquivos de log de eventos e músicas no banco Postgres.
    :param cur: referência para o cursor do banco
    :param conn: conexão do banco de dados
    :param filepath: diretório onde os arquivos estão
    :param func: função a ser chamada
    """
    # pega todos os arquivos no diretório passado
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # pega o total de arquivos encontrados
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # itera sobre os arquivos e processa os dados deles
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    
    
def main():
    """
    Função principal para carregar os arquivos de músicas e logs no banco de dados.
    """
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres')
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    conn.close()

if __name__ == '__main__':
    main()
    print(f'\n\n Finished processing!\n\n')