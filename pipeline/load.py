
from sqlalchemy import create_engine

def rdb_cursor_loader(db_connector, _query_list,_result):
    
    with db_connector as connected:
        cur= connected.conn.cursor()
        
        
        for _idx, _query in enumerate(_query_list['create'].values()):
            cur.executemany(_query,_result[_idx])
            connected.conn.commit()
    
    return print("데이터 이행이 완료 되었습니다.")


def rdb_pandas_loader(db_connector, _query_list, _name, _result):

    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'\
        .format(\
            user = db_connector.user
            , password = db_connector.password
            , host = db_connector.host
            , port = db_connector.port
            , database = db_connector.database)
            )

    for _idx, _dict in enumerate(zip(_query_list['read'].items())):
        _result[_idx].to_sql(_dict[0][0]+f'{_name}', engine, if_exists = 'append', index = False)

    return print("데이터 이행이 완료 되었습니다!!! 그런데... pandas로")


def rdb_pandas_loader_custom_table(db_connector, _name, _result):
    
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'\
        .format(\
            user = db_connector.user
            , password = db_connector.password
            , host = db_connector.host
            , port = db_connector.port
            , database = db_connector.database)
            )
    _result.to_sql(_name, engine, if_exists = 'append', index = False)       

    return print("수정된 데이터 이행이 완료 되었습니다!!! 그런데... pandas로")

from ast import literal_eval

def ddb_cursor_loader(db_connector, _query_list, _result):
    
    with db_connector as connected:
        

        for _idx, _dict in enumerate(zip(_query_list['read'].items())):
        
            _col= connected.conn.get_collection(_dict[0][0])
            _col.insert_many(_result[_idx])         
        
    return print("데이터 이행이 완료 되었습니다. 그런데 MongoDB로")

def ddb_only_one_loader(db_connector,_result,_name):
    
    with db_connector as connected:
        
        _col = connected.conn.get_collection(_name)
        _col.insert_many(_result)
    
    return print("데이터 이행이 완료 되었습니다. 그런데 MongoDB로")