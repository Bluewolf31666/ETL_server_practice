from pipeline.controller import etl_1,etl_2,etl_3,etl_4,etl_0,etl_5

import click
from datetime import datetime, timedelta
import sys

@click.command()
@click.option('-m', '--custom-batch-month', type = click.STRING, default='', help='배치작업연월') #다붙어있어야함
def start_batch(custom_batch_month):
    
    batch_month=_get_batch_month(custom_batch_month)
    print('get_batch_month : ',batch_month)
    _yyyymm=_check_valid_month(custom_batch_month)
    print('input :',custom_batch_month)
    print('check output : ',_yyyymm)
    
    if not batch_month:
        print('batch_month is None')
        sys.exit(1)
    try: 
        #etl_2(batch_month)
        #etl_3(batch_month)
        #etl_4(batch_month)
        #etl_0()
        etl_5(batch_month)
    except Exception as e:
        print(e)
        sys.exit(1)
    sys.exit(0) 
    
def _get_batch_month(_custom_batch_month):
    if _custom_batch_month:
        print('custom_batch > batch month', _custom_batch_month)
        return _check_valid_month(_custom_batch_month)
    
    first_day = datetime.today().replace(day=1)
    batch_month = first_day- timedelta(days= 1)
    return batch_month.strftime('%Y%m')


    
def _check_valid_month(str_yyyymm):
    try:
        #print(str_yyyymm)
        datetime.strptime(str_yyyymm, '%Y%m')
        return str_yyyymm
    except Exception as e:
        return None

if __name__ == '__main__':
    print("start_batch_job")
    start_batch()
    print("end_batch_job")


