from db.connector import DBConnector
from db.queries_rdb import queries_rdb,queries_job2,queries_job1
from db.querries_ddb import queries_ddb
from pipeline import extract, transform, load

from settings import DB_SETTINGS



def etl_1(_yyyymm):
    print('start_etl_1')
    print(_yyyymm)
    result= extract.rdb_cursor_extractor(db_connector=DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
                                        ,_query_list=queries_rdb)

    load.rdb_cursor_loader(db_connector = DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
                    _query_list= queries_rdb,
                    _result= result
                    )
    print('end_etl_1')
    
def etl_2(_yyyymm):
    print('start_etl_2')
    print(_yyyymm)
    result= extract.rdb_pandas_extractor(db_connector=DBConnector(**DB_SETTINGS['source_db_localhost_rdb']),
    _query_list=queries_job2,param={'batch_month':_yyyymm})
    load.rdb_pandas_loader(
        db_connector=DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
        _query_list=queries_job2,
        _name='',
        _result=result
    )
    #final=transform.merge_data(result)
    print('end_etl_2')
    
def etl_3(_yyyymm):
    print('start_etl_3')
    result= extract.rdb_pandas_extractor(db_connector=DBConnector(**DB_SETTINGS['source_db_localhost_rdb']),
    _query_list=queries_job2,param={'batch_month':_yyyymm})
    #actor film, film 액터를 갖고 와서 transform 모듈을 돌리고,temp0에 적재할 것이다.
    #load module에 rdb_pandas_loader는 쿼리에 종속됨.
    #새로 만들어지는 생성되는 write하기 위한 rdb_pandas_loader_custom_table를 만들고
    #테이블의 name을 지정해서 목적지가 되는 데이터 베이스에 insert하는 함수를 구현해서 controller에 붙히고 메인에 호출해서 해보기
    transform_table=transform.merge_data(result)
    load.rdb_pandas_loader_custom_table(
        db_connector=DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
        _name='test1',
        _result=transform_table
    )

def etl_4(_yyyymm):
    print('start_etl_4')
    result=extract.ddb_cursor_extractor(db_connector=DBConnector(**DB_SETTINGS['source_db_localhost_ddb']), _query_list=queries_ddb)
    load.ddb_cursor_loader(
        db_connector=DBConnector(**DB_SETTINGS['target_db_localhost_ddb']),
        _query_list = queries_ddb,
        _result=result
    )
    print('end_etl_4')
    
    
def etl_0():
    print('start_etl_0')
    result=extract.ddb_cursor_extractor(db_connector=DBConnector(**DB_SETTINGS['source_db_localhost_ddb']), _query_list=queries_ddb)
    result2=transform.ddb_to_rdb(result[0])
    #print(result2)
    load.rdb_pandas_loader_custom_table(db_connector=DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),_result=result2,_name='bk_list')
    print('end_etl_0')


def etl_5(_yyyymm):
    print('start_etl_5')
    result=extract.rdb_pandas_extractor(db_connector=DBConnector(**DB_SETTINGS['source_db_localhost_rdb']),
    _query_list=queries_job2,param={'batch_month':_yyyymm})
    result2=transform.rdb_to_ddb(result)
    load.ddb_only_one_loader(db_connector=DBConnector(**DB_SETTINGS['target_db_localhost_ddb']),_result=result2,_name='test')