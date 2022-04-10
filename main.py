import os
import asyncio
import sys
import threading
import time
import logging

from metaapi_cloud_sdk import MetaApi
from datetime import datetime, timedelta
import mysql.connector

token = os.getenv(
    'TOKEN') or 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI3N2EzN2QzZTcyZmY1MGUyNzNkYWUzNGUwZWM5MWUxYiIsInBlcm1pc3Npb25zIjpbXSwidG9rZW5JZCI6IjIwMjEwMjEzIiwiaWF0IjoxNjQ2OTk2ODMzLCJyZWFsVXNlcklkIjoiNzdhMzdkM2U3MmZmNTBlMjczZGFlMzRlMGVjOTFlMWIifQ.g1wh3Grf8MEKaW1lB9fF1QKdCFQREiAS7yxtCL18pLoHXX4TArYTJQCAP3KzxLeCu_ZU92fY7qFwxVqN-_VLuBUQjFw-b5aDPU3NIU95lO6sny-6EWZH7-CrnxnfalwNWWUd_gLGzbOROLczHcEZ8VYYFdxqmpsBV1XR1iGAIlBn35w6tcnOmH54cuCfGiHDRFHlljFuSIUY0Gxys-lA3YV6ewO0vC7Q_Cl0CY7aaepIhm42dIo82rJaISvIgTNG58JuvAMiCLm2ukEqBxKAVNh2EOqmB3DjcuJk8AqoMPDARu5qiItJ2Ow2xs4CxY3SimLBF2lHkfLzoJ-aoG5c9jPNn2GT79UKK3P9ddma0amUfVskLQl-tcAHrHuFHNsYhbr9EGi6_d7RnznQ7pRTuDYIyAXl3-eb0ztQUMXWzCCPaL86pB6QUBglAjibiXqihij6POnVpQVvbpVoKgkbZsviEJ4Zcub_5gVBtmoZBNHXwwy6izTOGyPmHfHjRRj4OTufWQLkPz-GDNxYgc3Bz3JAEB-PFR1kf9b5LONiie-2dCoTUH87vIUr2G-ynHZlFBnSB_xrLytKao6iG1h2SND4lsWLPkD4W6uIauxdCJyN1RDRhx-QjJovKQ-gubjradm105RygtrbYYZ19QDYkVTVpl4xGTWl6KfmW4zc1IQ'

logging.basicConfig(level=logging.WARNING, filename='app.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#host = "database-fx.coh7rhckhujc.us-east-1.rds.amazonaws.com"
#user = "overviewfx"
host = '192.168.1.57'
user = "alessandro"

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

def stopWorking(login):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        stopcursor = mydb.cursor()
        update = f"UPDATE `Accounts` SET `running` = '0' WHERE `Accounts`.`name` = %s"
        val = (login,)
        stopcursor.execute(update, val)
        mydb.commit()
        stopcursor.close()
        mydb.close()
    except Exception as err:
        sys.exit(err)

def new_Process(token, tabella, login, password, platform, server_name, type):
    asyncio.run(meta_api_synchronization(token, tabella, login, password, platform, server_name, type))
    sys.exit()


async def meta_api_synchronization(token, tabella, login, password, platform, server_name, type):
    api = MetaApi(token)
    try:
        # Add test MetaTrader account
        accounts = await api.metatrader_account_api.get_accounts()
        account = None
        for item in accounts:
            if item.login == login and item.type.startswith('cloud'):
                account = item
                break
        if not account:
            account = await api.metatrader_account_api.create_account({
                'name': tabella,
                'type': 'cloud',
                'login': login,
                'password': password,
                'server': server_name,
                'platform': platform,
                'application': 'MetaApi',
                'magic': 1000
            })
        #  wait until account is deployed and connected to broker
        logging.debug('Deploying account')
        await account.deploy()
        logging.debug('Waiting for API server to connect to broker (may take couple of minutes)')
        await account.wait_connected()
        connection = account.get_rpc_connection()
        while True:
            try:
                accountInfo = await connection.get_account_information()
                daemonProcess = threading.Thread(target=saveEquity, args=(accountInfo, tabella, type))
                daemonProcess.start()
                daemonProcess.join()
                await asyncio.sleep(60)
            except Exception as err:
                logging.error(f"Errore account{login} : {api.format_error(err)} , riavviando")
                await asyncio.sleep(60)
                await account.wait_connected()
                connection = account.get_rpc_connection()
                pass

    except Exception as err:
        # process errors
        if hasattr(err, 'details'):
            if err.details == 'E_SRV_NOT_FOUND':
                logging.error(err)
            elif err.details == 'E_AUTH':
                logging.error(err)
            elif err.details == 'E_SERVER_TIMEZONE':
                logging.error(err)
        logging.error(api.format_error(err))
        stopWorking(login)
    exit()


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


def restartProcess(runningAccounts):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        restartcursor = mydb.cursor()
        testTime = datetime.utcnow() - timedelta(minutes=3)
        query = f"SELECT name FROM `Accounts` WHERE lastUpdate < %s and active = 1"
        restartcursor.execute(query, (testTime.__str__(),))
        for account in mycursor.fetchall():
            terminatedAccount = runningAccounts.pop(account[0])
            # terminatedAccount.terminate()
            time.sleep(0.1)
            if not terminatedAccount.is_alive():
                terminatedAccount.join(timeout=1.0)
                logging.warning(f"riavviando {terminatedAccount}")
            else:
                logging.error(f"error terminating {terminatedAccount}")
            stopWorking(account[0])
            retrieveProcess(account[0], runningAccounts)
        restartcursor.close()
        mydb.close()
    except Exception as err:
        logging.error(err)
        pass


def retrieveProcess(login, runningAccounts):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        retrievecursor = mydb.cursor()
        query = "SELECT name,tabella,password,Piattaforma,Server,type FROM Accounts WHERE name = %s"
        retrievecursor.execute(query, (login,))
        login, tabella, password, Piattaforma, Server, type = retrievecursor.fetchone()
        createProcess(login, tabella, password, Piattaforma, Server, type, runningAccounts)
        retrievecursor.close()
        mydb.close()
    except Exception as err:
        logging.error(err)
        pass


def createProcess(login, tabella, password, Piattaforma, Server, type, runningAccounts):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        innercursor = mydb.cursor()
        test = f" SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tabella}' "
        innercursor.execute(test)
        if innercursor.fetchone()[0] != 1:
            logging.info(f"creando la tabella {tabella}")
            create = f"CREATE TABLE `{tabella}` ( `timestamp` DATETIME NOT NULL , `equity` INT NOT NULL , `balance` INT NOT NULL , `equitygap` INT NOT NULL, `maxBalance` INT NOT NULL  , PRIMARY KEY (`timestamp`)) ENGINE = InnoDB;"
            innercursor.execute(create)
            mydb.commit()
        test = f"SELECT COUNT(*) FROM dailyMax WHERE name = %s"
        innercursor.execute(test, (tabella,))
        if innercursor.fetchone()[0] != 1:
            logging.info(f"creando il maxBalance {tabella}")
            create = f"INSERT INTO dailyMax (name,maxBalance) VALUES (%s,%s)"
            innercursor.execute(create, (tabella, 0))
            mydb.commit()
        daemonProcess = threading.Thread(target=new_Process,
                                         args=(token, tabella, login, password, Piattaforma.lower(), Server, type))
        daemonProcess.start()
        runningAccounts[login] = daemonProcess
        update = f"UPDATE `Accounts` SET `running` = '1' WHERE `Accounts`.`name` = %s"
        innercursor.execute(update, (login,))
        mydb.commit()
        innercursor.close()
        mydb.close()
    except Exception as err:
        logging.error(err)
        pass


def new_Remover(token, login):
    asyncio.run(meta_api_remover(token, login))
    sys.exit()


async def meta_api_remover(token, login):
    api = MetaApi(token)
    try:
        accounts = await api.metatrader_account_api.get_accounts()
        account = None
        for item in accounts:
            if item.login == login and item.type.startswith('cloud'):
                account = item
                break
        if not account:
            logging.error("Errore Cancellazione!")
            exit(-1)
        await account.undeploy()
        #await account.remove()
        try:
            mydb = mysql.connector.connect(
                host=host,
                user=user,
                password="scricciolo",
                database="MetaApi"
            )
            update = f"UPDATE `Accounts` SET `active` = '0',status = 'STOP' WHERE name = '{login}'"
            resetcursor = mydb.cursor()
            resetcursor.execute(update)
            mydb.commit()
            resetcursor.close()
            mydb.close()
            logging.error(f"Removed {login} successfully")
        except Exception as err:
            logging.error(err)
            pass
    except Exception as err:
        # process errors
        if hasattr(err, 'details'):
            if err.details == 'E_SRV_NOT_FOUND':
                logging.error(err)
            elif err.details == 'E_AUTH':
                logging.error(err)
            elif err.details == 'E_SERVER_TIMEZONE':
                logging.error(err)
        logging.error(api.format_error(err))
        stopWorking(login)
    exit()


def SQL_Remover(login):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        update = f"UPDATE `Accounts` SET `active` = '0',status = 'STOP' WHERE name = '{login}'"
        resetcursor = mydb.cursor()
        resetcursor.execute(update)
        mydb.commit()
        resetcursor.close()
        mydb.close()
    except Exception as err:
        logging.error(err)
        pass

def SQL_statistics(item,deals):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password="scricciolo",
            database="MetaApi"
        )
        positions = dict()
        dictdeals = dict()
        for deal in deals.get("deals"):
            if (deal.get('type') == 'DEAL_TYPE_BALANCE'):
                continue
            if dictdeals.__contains__(deal.get('id')):
                dictdeals[deal.get('id')] = dictdeals[deal.get('id')] + deal.get("profit")
            else:
                dictdeals[deal.get('id')] = deal.get("profit")
            if not positions.keys().__contains__(deal.get('positionId')):
                positions[deal.get('positionId')] = deal.get("profit")
            else:
                positions[deal.get('positionId')] = positions.get(deal.get('positionId')) + deal.get(
                    "profit")
        tradesNumber = dictdeals.__len__()
        positiveTrades = sum(list(map(lambda x: x > 0 and 1 or 0, dictdeals.values())))
        totalProfit = sum(list(map(lambda x: x > 0 and x or 0, dictdeals.values())))
        totalLoss = sum(list(map(lambda x: x < 0 and x or 0, dictdeals.values())))
        if positiveTrades != 0:
            averageprofit = totalProfit / positiveTrades
        else:
            averageprofit = 0

        if positiveTrades != tradesNumber:
            averageloss = totalLoss / (tradesNumber - positiveTrades)
            winratio = positiveTrades / (tradesNumber - positiveTrades)
        else:
            averageloss = 0
            winratio = 1

        if totalLoss != 0:
            profitfactor = abs(totalProfit / totalLoss)
        else:
            profitfactor = 1

        if tradesNumber != 0 and positiveTrades != 0 and tradesNumber - positiveTrades != 0:
            expectancy = ((positiveTrades / tradesNumber) * (totalProfit / positiveTrades)) - (
                    ((tradesNumber - positiveTrades) / tradesNumber) * abs(
                totalLoss / (tradesNumber - positiveTrades)))
        else:
            expectancy = 0

        try:
            mycursor = mydb.cursor()
            query = "SELECT COUNT(*) FROM Statistics WHERE account = %s"
            mycursor.execute(query, (item.login,))
            if mycursor.fetchone() != 0:
                mycursor.execute("DELETE FROM Statistics WHERE account = %s", (item.login,))
            query = "INSERT INTO Statistics (account , trades , wintrades , totwin , totloss , expectancy , winratio , profitfactor , avgwin , avgloss,timestamp ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(query, (
                item.login, tradesNumber, positiveTrades, totalProfit, abs(totalLoss), expectancy,
                round(winratio, 2),
                round(profitfactor, 2), averageprofit, abs(averageloss),
                datetime.utcnow().replace(microsecond=0, second=0)))
            mydb.commit()
            # print("inserimento riuscito")
        except Exception as err:
            logging.error("inserimento fallito !")
            logging.error(err)
            pass
        mydb.close()
    except Exception as err:
        logging.error("inserimento fallito !")
        logging.error(err)
        pass


async def meta_api_statistics(token):
    api = MetaApi(token)
    start_time = datetime.utcnow() - timedelta(days=30)
    end_time = datetime.utcnow()
    try:
        accounts = await api.metatrader_account_api.get_accounts()
        for item in accounts:
            try:
                logging.warning(item.state)
                if item.state.__eq__('DEPLOYED'):
                    connection = item.get_rpc_connection()
                    await connection.wait_synchronized()
                    deals = await connection.get_deals_by_time_range(start_time=start_time, end_time=end_time)
                    # daemon = threading.Thread(target=SQL_statistics,args=(item, deals))
                    # daemon.start()
                    # daemon.join()
                    SQL_statistics(item, deals)
            except Exception as err:
                logging.error(err)
                pass

    except Exception as err:
        # process errors
        if hasattr(err, 'details'):
            if err.details == 'E_SRV_NOT_FOUND':
                logging.error(err)
            elif err.details == 'E_AUTH':
                logging.error(err)
            elif err.details == 'E_SERVER_TIMEZONE':
                logging.error(err)
        logging.error(f"errore qua : {api.format_error(err)}")
        pass

def new_Stats(token):
    asyncio.run(meta_api_statistics(token))
    sys.exit()

def statistics():
    sleeptime = 60
    while True:
        try:
            #asyncio.run(meta_api_statistics(token))
            daemonProcess = threading.Thread(target=new_Stats, args=(token,))
            daemonProcess.start()
            daemonProcess.join()
        except Exception as err:
            logging.error(err)
            pass
        time.sleep(sleeptime)

if __name__ == '__main__':
    reset()
    threading.Thread(target=statistics).start()
    runningAccounts = dict()
    while True:
        try:
            mydb = mysql.connector.connect(
                host=host,
                user=user,
                password="scricciolo",
                database="MetaApi"
            )
            mycursor = mydb.cursor()
            query = f"SELECT name,tabella,password,Piattaforma,Server,type FROM `Accounts` WHERE active = 1 and running = 0"
            mycursor.execute(query)
            for row in mycursor.fetchall():
                logging.info(f"main: {row}")
                login, tabella, password, Piattaforma, Server, type = row
                createProcess(login, tabella, password, Piattaforma, Server, type, runningAccounts)
                time.sleep(1)
            query = f"SELECT name,tabella,password,Piattaforma,Server,type FROM `Accounts` WHERE status = 'CANC'"
            mycursor.execute(query)
            for row in mycursor.fetchall():
                login, tabella, password, Piattaforma, Server, type = row
                logging.warning(f"stopping{login}")
                daemonProcess = threading.Thread(target=new_Remover,
                                                 args=(token, login))
                daemonProcess.start()
                daemonProcess.join()
            logging.info(f"Current accounts : {runningAccounts}")
            mydb.commit()
            mycursor.close()
            mydb.close()
            for account in runningAccounts.values():
                account.join(timeout=0.1)
            time.sleep(60)
            restartProcess(runningAccounts)
        except Exception as err:
            logging.error(err)
            pass