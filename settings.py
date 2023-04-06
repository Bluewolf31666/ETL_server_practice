DB_SETTINGS = {
    'source_db_localhost_rdb': {
        'host' : "127.0.0.1",
        'port' : "5432",
        'user' : "postgres",
        'password' : "admin",
        'database' : "dvdrental",
        'location' : "localhost_source",
        'engine' : "postgre"
    },
    'source_db_localhost_ddb' : {
        'host' : "127.0.0.1",
        'port' : 27017,
        'user' : "root",
        'password' : "root",
        'database' : 'yes24',
        'location' : "localhost_source",
        'engine' : "mongodb"
    },
    'target_db_localhost_rdb' : {
        'host' : "127.0.0.1",
        'port' : "5432",
        'user' : "postgres",
        'password' : "admin",
        'database' : "temp0",
        'location' : "localhost_target",
        'engine' : "postgre"
    },
    'target_db_localhost_ddb' : {
        'host' : "127.0.0.1",
        'port' : 27017,
        'user' : "root",
        'password' : "root",
        'database' : "temp0",
        'location' : "localhost_target",
        'engine' : "mongodb"
    },
}