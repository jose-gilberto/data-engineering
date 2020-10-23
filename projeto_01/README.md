# Modelagem de dados com o Postgres

## Visão geral

Nesse projeto, aplicamos modelagem de dados com o Postgres e construímos uma pipeline ETL usando Python. Uma Startup quer analisar os dados que eles vem coletando de músicas e atividades dos usuários no seu novo aplicativo de streaming de músicas. Atualmente, eles coletam os dados no formato json e o time de análise está particularmente interessado em entender que músicas os usuários estão escutando.

## Dataset de músicas

O dataset de músicas é um subset do [Million Song Dataset](http://millionsongdataset.com/).

Exemplo de registro:
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

Com formatação:
```json
{
  "num_songs": 1, 
  "artist_id": "ARJIE2Y1187B994AB7", 
  "artist_latitude": null, 
  "artist_longitude": null, 
  "artist_location": "", 
  "artist_name": "Line Renaud", 
  "song_id": "SOUPIRU12A6D4FA1E1", 
  "title": "Der Kleine Dompfaff", 
  "duration": 152.92036, 
  "year": 0
}
```

## Dataset de Log

O dataset de logs é gerado pelo [Event Simulator](https://github.com/Interana/eventsim).

Exemplo de registro:
```json
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET","page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}
```

Com formatação:
```json
{
  "artist": null, 
  "auth": "Logged In", 
  "firstName": "Walter", 
  "gender": "M", 
  "itemInSession": 0, 
  "lastName": "Frye", 
  "length": null, 
  "level": "free", 
  "location": "San Francisco-Oakland-Hayward, CA", 
  "method": "GET",
  "page": "Home", 
  "registration": 1540919166796.0, 
  "sessionId": 38, 
  "song": null, 
  "status": 200, 
  "ts": 1541105830796, 
  "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", 
  "userId": "39"
}
```

## Schema

Utilizaremos um star schema, leia mais [aqui](https://rafaelpiton.com.br/blog/data-warehouse-star-schema/).

### Fact table

**songplays** - registros em dados de log associados com reproduções de música, ou seja, registros com página `NextSong`.
```songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent```

### Dimension tables

**users** - usuários no aplicativo.
```user_id, first_name, last_name, gender, level```

**songs** - músicas no banco de dados.
```song_id, title, artist_id, year, duration```

**artists** - artistas no banco de dados.
```artist_id, name, location, latitude, longitude```

**time** - timestamp dos registros em canções divididos em unidades específicas.
```start_time, hour, day, week, month, year, weekday```

## Arquivos do projeto

## Ambiente

## Execução