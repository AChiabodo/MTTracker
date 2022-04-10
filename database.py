
host = '192.168.1.57'
user = "alessandro"


from datetime import datetime, timedelta
import mysql.connector
import logging
import sys


logging.basicConfig(level=logging.WARNING, filename='app.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def saveEquity(accountInfo, tabella, type):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        mycursor = mydb.cursor()
        query = "SELECT maxBalance, TIMESTAMP FROM dailyMax WHERE name = %s"
        mycursor.execute(query, (tabella,))
        max, timestamp = mycursor.fetchone()
        equity = accountInfo.get('equity')
        balance = accountInfo.get('balance')
        e_gap = round(equity - balance, 2)
        time = datetime.utcnow().replace(microsecond=0, second=0)
        print(f"max : {max} | balance : {balance}")
        if balance > max or timestamp.day < time.day:
            # aggiorno il dailyMax
            print("aggiorno il maxBalance")
            max = balance
            update = f"UPDATE dailyMax SET maxBalance = %s,TIMESTAMP = %s WHERE name = %s"
            mycursor.execute(update, (max, str(time), tabella))
            mydb.commit()
        sql = f"INSERT INTO {tabella} (timestamp, equity, balance, equitygap,maxBalance) VALUES (%s, %s, %s ,%s,%s)"
        val = (time.__str__(), equity, balance, e_gap, max)
        mycursor.execute(sql, val)
        mydb.commit()
        sql = f"UPDATE Accounts SET lastUpdate = %s WHERE tabella = %s"
        val = (time.__str__(), tabella)
        mycursor.execute(sql, val)
        mydb.commit()
        fail = f"UPDATE Accounts SET STATUS = 'FAIL', reason = %s WHERE tabella = %s and STATUS ='OK'"
        if (equity < type * 0.9):
            mycursor.execute(fail, (f"10% @{time.__str__()}", tabella))
            mydb.commit()
        elif (equity < max - (type * 0.05)):
            mycursor.execute(fail, (f"5% @{time.__str__()}", tabella))
            mydb.commit()
        mycursor.close()
        mydb.close()
    except Exception as err:
        logging.error(err)
        sys.exit(err)


def reset():
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        update = f"UPDATE `Accounts` SET `running` = '0' WHERE active = 1"
        resetcursor = mydb.cursor()
        resetcursor.execute(update)
        mydb.commit()
        resetcursor.close()
        mydb.close()
    except Exception as err:
        logging.error(err)
        pass
