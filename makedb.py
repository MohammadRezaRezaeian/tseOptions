import os
import pandas as pd
import sqlite3

files_str= os.listdir('./tsetmcdata')

# for file_str in files_str:
#     if file_str.endswith('.xlsx'):
#         data= pd.read_excel('./tsetmcdata/'+file_str, skiprows=2)
#         # print(data)
#         # print(data.columns)
#         options= data.loc[data['نام'].str.contains('اختيار')].set_index(['نماد'])
#         # print(options)
#         print(file_str)
#         options.to_excel('./tsetmcOptions/'+file_str)


def insert_symbol(ticker, name):
    conn= sqlite3.connect('./db/options.db')
    cur= conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS tickers
                (ID             INTEGER     PRIMARY KEY         AUTOINCREMENT,
                ticker          TEXT                            NOT NULL,
                name            TEXT                            NOT NULL)
                '''
                )
    sql= f'''INSERT INTO tickers (ticker, name)
                 SELECT ?,? WHERE NOT EXISTS
                 (SELECT ticker FROM tickers WHERE ticker=? AND name=?)
                 '''
    cur.execute(sql,(ticker,name,ticker,name))
    conn.commit()
    conn.close()


def insert_prices(date,tickerID,number,volume,value,yesterday,
                first,last,change,changePrc,close,closeChange,closeChangePrc,low,high):
    conn= sqlite3.connect('./db/options.db')
    cur= conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS prices
                (IND             INTEGER         PRIMARY KEY        AUTOINCREMENT,
                date               DATE                               NOT NULL,
                tickerID           INTEGER                            NOT NULL,
                number             INTEGER                            NOT NULL,
                volume             DOUBLE                             NOT NULL,
                value              DOUBLE                             NOT NULL,
                yesterday          DOUBLE                             NOT NULL,
                first              DOUBLE                             NOT NULL,
                last               DOUBLE                             NOT NULL,
                change             DOUBLE                             NOT NULL,
                changePrc          DOUBLE                             NOT NULL,
                close              DOUBLE                             NOT NULL,
                closeChange        DOUBLE                             NOT NULL,
                closeChangePrc     DOUBLE                             NOT NULL,
                low                DOUBLE                             NOT NULL,
                high               DOUBLE                             NOT NULL)
                '''
                )
    sql= f'''INSERT INTO prices (date , tickerID, number, volume, value, yesterday,
                first, last, change, changePrc, close, closeChange, closeChangePrc, low, high)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                 '''
    cur.execute(sql,(date,tickerID,number,volume,value,yesterday,
                first,last,change,changePrc,close,closeChange,closeChangePrc,low,high))
    conn.commit()
    conn.close()

def get_symbolId(symbol):
    conn= sqlite3.connect('./db/options.db')
    cur= conn.cursor()
    cur.execute('SELECT ID FROM tickers WHERE ticker=?',(symbol,))
    id=cur.fetchall()
    conn.close()
    return id[0][0]
    
for file_str in files_str:
    if file_str.endswith('.xlsx'):
        data= pd.read_excel('./tsetmcOptions/'+file_str)
        for row in data.iterrows():
            # print(row[1]['نماد'], row[1]['نام'])
            insert_symbol(row[1]['نماد'], row[1]['نام'])
            number,volume,value,yesterday,first,\
                last,change,changePrc,close,cCange,cCangePrc,low,high=[row[1][col] for col in data.columns[2:]]
            sym=get_symbolId(row[1]['نماد'])
            insert_prices(file_str[:-5],sym , number, volume, value, yesterday, first,
                          last, change, changePrc, close, cCange, cCangePrc, low, high)


# conn= sqlite3.connect('./db/options.db')
# cursor = conn.execute("SELECT * from tickers")
 
# for row in cursor:
#     print(row)
