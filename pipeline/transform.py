import pandas  as pd

def merge_data(result_table):
    first_data=pd.merge(result_table[2],result_table[0], how='inner', on='actor_id')
    result_data=pd.merge(first_data,result_table[1], how='inner', on='film_id')
    return result_data

def ddb_to_rdb(_result):
    return pd.DataFrame(_result)

def rdb_to_ddb(_result):
    result=[]
    _to_dict=_result[0].to_dict(orient = 'index')
    for _key, _value in _to_dict.items():
        result.append(_value)
    
    return _to_dict.values()