#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
import datetime

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.backtest.rate_stats as rate
from instock.core.singleton_stock import stock_hist_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def prepare_old():
    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns

    stocks_data = stock_hist_data().get_data()
    if stocks_data is None:
        return
    for k in stocks_data:
        date = k[0]
        break
    # 回归测试表
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            executor.submit(process, table, stocks_data, date, backtest_column)

def pandas_df(conn, sql):
    df = pd.read_sql(sql, conn)
    # conn.close()
    return df

def stocks_data_to_realtime(date,stocks_data,realdf):
    realdf['open'] = realdf['open_price']
    realdf['high'] = realdf['high_price']
    realdf['low'] = realdf['low_price']
    realdf['quote_change'] = realdf['change_rate']
    realdf['lastp'] = realdf['pre_close_price']
    # realdf['close'] = realdf['pre_close_price']
    realdf['close'] = realdf['new_price']
    realdf['amount'] = realdf['deal_amount']
    realdf['turnover'] = realdf['turnoverrate']
    realdf['volume'] = realdf['volume'].apply(lambda x: x*100)
    realdf['amount'] = realdf['amount'].apply(lambda x: x*100)
    
    h_col = tuple(tbs.CN_STOCK_HIST_DATA['columns'])
    # h_col.append('p_change')
    # ('2023-05-11', '603058', '永吉股份')
    stocks_data2={}
    rundate = date.strftime("%Y-%m-%d")
    # rundate = date
    for key in stocks_data:
        # date1 = key[0]
        code  = key[1]
        name = key[2]
        pr_value = stocks_data[key]
        pr_value = pr_value[pr_value.date < str(date)[:10]]
        # scol = stocks_data[key].columns.values
        data = realdf.loc[realdf.code==code,h_col]
        # pr_value = pr_value.append(data).reset_index(drop=True)
        
        #debug realtime
        # pr_value = pr_value[:-1]
        #debug realtime
        pr_value = pd.concat([pr_value, data], axis=0).reset_index(drop=True)
        pr_value.loc[:, 'p_change'] = tl.ROC(pr_value['close'].values, 1)
        pr_value['date'] = pd.to_datetime(pr_value.date, format='%Y-%m-%d')
        stocks_data2[(rundate,code,name)] = pr_value
        
    return stocks_data2

from instock.core.stockfetch import fetch_stocks
import talib as tl
import instock.lib.trade_time as trd
import datetime
import numpy as np
import pandas as pd

def get_rates(code_name, data, stock_column, threshold=101,realtime=True):
    try:
        # 增加空判断，如果是空返回 0 数据。
        if data is None:
            return None

        start_date = code_name[0]
        code = code_name[1]
        # 设置返回数组。
        stock_data_list = [start_date, code]

        mask = (data['date'] >= start_date)
        data2 = data.loc[mask].copy()
        data2 = data2.head(n=threshold)

        if len(data2.index) <= 1:
            if realtime:
                data = data[-2:].copy()
            else:
                return None
        else:
            data = data2
        close1 = data.iloc[0]['close']
        # data.loc[:, 'sum_pct_change'] = data['close'].apply(lambda x: round(100 * (x - close1) / close1, 2))
        data.loc[:, 'sum_pct_change'] = np.around(100 * (data['close'].values - close1) / close1, decimals=2)
        # 计算区间最高、最低价格

        first = True
        col_len = len(data.columns)
        for row in data.values:
            if first:
                first = False
            else:
                stock_data_list.append(row[col_len-1])

        _l = len(stock_column) - len(stock_data_list)
        for i in range(0, _l):
            stock_data_list.append(None)

    except Exception as e:
        logging.error(f"rate_stats.get_rates处理异常：{code}代码{e}")

    return pd.Series(stock_data_list, index=stock_column)

def prepareRealTime(stocks_data=None,realtime=False):
    tables = []
    tables.extend([tbs.TABLE_CN_STOCK_STRATEGIES[0]])
    tables.extend([tbs.TABLE_CN_STOCK_STRATEGIES[1]])
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns

    # logging.info(f"strategy_enter_readldf.prepare处理：{tables}")
    if stocks_data is None:
        run_date, run_date_nph = trd.get_trade_date_last()
        stocks_data = stock_hist_data(run_date).get_data()
    else:
        realtime = True
    if stocks_data is None:
        return
    for k in stocks_data:
        date = k[0]
        break
    # 回归测试表
    now_time = datetime.datetime.now()
    run_date = now_time.date()
            
    if date != str(run_date)[:10] and trd.is_trade_date(run_date) and trd.is_open(now_time) and not trd.is_close(now_time):
            
        date = run_date
        logging.info(f"strategy_enter_readldf：{date}")
        if not realtime:
            real_table_name = 'cn_stock_spot'
            read_sql = f"SELECT * FROM `{real_table_name}` where `date` = '{date}'"
            realdf = pandas_df(mdb.engine(),read_sql)
            if len(realdf) == 0:
                logging.info(f"strategy_enter_readldf.prepare处理异常：{date}")
                realdf = fetch_stocks(date)
            stocks_data = stocks_data_to_realtime(date,stocks_data,realdf)
            logging.info(f"realtime_enter_readldf：{list(stocks_data.keys())[0]}")
            
    # else:
    #     logging.info(f"realtime_is Not：{(date),(now_time)}")     
    #     return
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            executor.submit(process, table, stocks_data, date, backtest_column)
            

def process(table, data_all, date, backtest_column):
    table_name = table['name']
    if not mdb.checkTableIsExist(table_name):
        return

    column_tail = tuple(table['columns'])[-1]
    now_date = datetime.datetime.now().date()
    #today
    sql = f"SELECT * FROM `{table_name}` WHERE `date` > '{now_date}'- INTERVAL 10 DAY AND `{column_tail}` is NULL"
    try:
        data = pd.read_sql(sql=sql, con=mdb.get_connection())
        if data is None or len(data.index) == 0:
            return

        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        # subset['date'] = subset['date'].values.astype('str')
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]

        #realtime
        date = str(date)[:10]
        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            logging.info(f"backtest_results:None")
            return
        data_new = pd.DataFrame(results.values())
        logging.info(f"backtest_results:{data_new.loc[:,['date','code','rate_1']][-1:].values}")
        mdb.update_db_from_df(data_new, table_name, ('date', 'code'))

    except Exception as e:
        logging.error(f"backtest_data_daily_job.process处理异常：{table}表{e}")


def run_check(stocks, data_all, date, backtest_column, workers=40):
    data = {}
    try:
        #debug
        # for stock in stocks:
        #     print(stock)
        #     data = (get_rates(stock,data_all.get((date, stock[1], stock[2])),backtest_column,len(backtest_column) - 1))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(get_rates, stock,
                                              data_all.get((date, stock[1], stock[2])), backtest_column,
                                              len(backtest_column) - 1): stock for stock in stocks}
                    
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                except Exception as e:
                    logging.error(f"backtest_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
    except Exception as e:
        logging.error(f"backtest_data_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data


def main():
    prepareRealTime()


# main函数入口
if __name__ == '__main__':
    main()
