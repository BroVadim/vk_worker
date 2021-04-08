import requests
from datetime import datetime as dt
import pyodbc
import os
import time
import re

class DbClient:
    def __init__(self,connectionString):
        self.connectionString = connectionString

    def createDb(self,conStrForCreateDb):
        try:
            if not os.path.exists("C:\Databases"):
                os.mkdir("C:\Databases")
            connection = pyodbc.connect(conStrForCreateDb,autocommit=True)
            createDbRequest = """CREATE DATABASE vkontakte ON PRIMARY (NAME=N'vkontakte',FILENAME=N'C:\\Databases\\vkontakte.mdf') LOG ON (NAME=N'vkontakte_log',FILENAME=N'C:\\Databases\\vkontakte_log.ldf')"""
            dbCursor = connection.cursor()
            dbCursor.execute(createDbRequest)
            connection.commit()
            connection.close()
            print("Database creation completed...")
        except Exception as error:
            print("Method failed with error:" + str(error))

    def createTables(self):
        createTablesRequests = ["""CREATE TABLE postsInfo(groupID int,postID int,postDate datetime,postText nvarchar(max),postStatus bit)""",
        """CREATE TABLE commentsInfo(commentId int,commentatorId int,postID int,postDate datetime,commentText nvarchar(MAX),userStatus bit,textStatus bit)""",
        """CREATE TABLE users (userId int,firstName nvarchar(30),lastName nvarchar(50),bDate date,city nvarchar(30),education nvarchar(100),phoneNumber nvarchar(max))"""]
        for request in createTablesRequests:
            connection = pyodbc.connect(self.connectionString)
            dbCursor = connection.cursor()
            dbCursor.execute(request)
            connection.commit()
            connection.close()
        print("Creation of work tables is completed...")

    def insertDataInPostInfo(self,postID,postDate,postText,groupID):
    	try:
    		sqlString = """IF NOT EXISTS(SELECT * FROM postsInfo WHERE postID=?) INSERT INTO postsInfo (groupID,postID,postDate,postText,postStatus) VALUES (?,?,?,?,0)"""
    		connection = pyodbc.connect(self.connectionString)
    		dbCursor = connection.cursor()
    		dbCursor.execute(sqlString,postID,groupID,postID,postDate,re.sub('\n',' ',postText))
    		connection.commit()
    		connection.close()
    	except Exception as error:
    		print(error)

    def insertDataInCommentsInfo(self,commentId,commentatorId,postID,postDate,commentText):
    	try:
    		sqlString = """IF NOT EXISTS(SELECT * FROM commentsInfo WHERE commentId=?)
    		INSERT INTO commentsInfo (commentId,commentatorId,postID,postDate,commentText,userStatus,textStatus) VALUES(?,?,?,?,?,0,0)"""
    		connection = pyodbc.connect(connectionString)
    		dbCursor = connection.cursor()
    		dbCursor.execute(sqlString, commentId,commentId,commentatorId,postID,postDate,re.sub('\n',' ',commentText))
    		connection.commit()
    		connection.close()
    	except Exception as error:
    		print("Error in insertDataInCommentsInfo " +str(error))

    def updatePostInfo(self, postID):
        sqlString = """update postsInfo set postStatus=1 where postID=?"""
        connection = pyodbc.connect(connectionString)
        dbCursor = connection.cursor()
        dbCursor.execute(sqlString,postID)
        connection.commit()
        connection.close()

class Vk:
    def __init__(self,token,connectionString):
        self.token = token
        self.connectionString = connectionString
        self.db =  DbClient(connectionString)

    def searchPosts(self,groupId):
        offset= 1900
        count= 95
        
        for i in range(0,9):
            params = {'owner_id':groupId,'offset':offset,'count':count,'filter':'all','extended':1,'access_token':self.token,'v':5.103}
            r = requests.get('https://api.vk.com/method/wall.get',params)
            for j in range(0,count):
                try:
                    id = r.json()['response']['items'][j]['id']
                    self.db.insertDataInPostInfo(r.json()['response']['items'][j]['id'], dt.fromtimestamp(r.json()['response']['items'][j]['date']).strftime('%Y%m%d %H:%M:%S'),r.json()['response']['items'][j]['text'],groupId)
                except:
                    continue
            offset += count
            time.sleep(2)

    def searchComments(self,groupId):
        groupId = int(groupId)
        sqlString = """select postID from postsInfo where groupID=? and postStatus=0"""
        connection = pyodbc.connect(self.connectionString)
        dbCursor = connection.cursor()
        dbCursor.execute(sqlString,groupId)
        for row in dbCursor:
            self.searchPostComments(groupId,row.postID,self.token)
            self.db.updatePostInfo(row.postID)
        connection.commit()
        connection.close()
        time.sleep(2)

    def searchPostComments(self,groupId,postID,userToken):
        params = {'owner_id':groupId,'post_id':postID,'need_likes':0,'offset':0,'count':95,'preview_lenght':0,'extended':1,'access_token':userToken,'v':5.103}
        r2 = requests.get('https://api.vk.com/method/wall.getComments',params)
        commentsCount = int(r2.json()['response']['count'])
        for i in range(0,commentsCount):
            try:
                countThreads = int(r2.json()['response']['items'][i]['thread']['count'])
                commentId = int(r2.json()['response']['items'][i]['id'])
                if countThreads !=0:
                    self.db.insertDataInCommentsInfo(int(r2.json()['response']['items'][i]['id']),int(r2.json()['response']['items'][i]['from_id']),int(postID),dt.fromtimestamp(r2.json()['response']['items'][i]['date']).strftime('%Y%m%d %H:%M:%S'),str(r2.json()['response']['items'][i]['text']))
                    self.writeThreadsComments(groupId,postID,commentId,userToken)
                else:
                    self.db.insertDataInCommentsInfo(int(r2.json()['response']['items'][i]['id']),int(r2.json()['response']['items'][i]['from_id']),int(postID),dt.fromtimestamp(r2.json()['response']['items'][i]['date']).strftime('%Y%m%d %H:%M:%S'),str(r2.json()['response']['items'][i]['text']))
            except Exception as error:
                print("Error in searchComments"+str(error))
                continue
        time.sleep(2)

    def writeThreadsComments(self,groupId,postId,commentId,userToken):
        params = {'owner_id':groupId,'post_id':postId,'comment_id':commentId,'need_likes':0,'offset':0,'count':20,'preview_lenght':0,'extended':1,'access_token':userToken,'v':5.103}
        r = requests.get('https://api.vk.com/method/wall.getComments',params)
        commentsCount = int(r.json()['response']['count'])
        for i in range(0,commentsCount):
            try:
                self.db.insertDataInCommentsInfo(int(r.json()['response']['items'][i]['id']),int(r.json()['response']['items'][i]['from_id']),int(postId),dt.fromtimestamp(r.json()['response']['items'][i]['date']).strftime('%Y%m%d %H:%M:%S'),str(r.json()['response']['items'][i]['text']))
            except Exception as error:
                print("Error in writeThreadsComments."+str(error))
                continue
        time.sleep(2)

#https://oauth.vk.com/authorize?client_id=7259373&display=page&redirect_uri=https://oauth.vk.com/blank.html&scope=friends,status,wall,groups,stats&response_type=token&v=5.63
class Main:
    def __init__(self,groupId,token,conStrForCreate,connectionString):
        self.id = groupId
        self.token = token
        self.conStrForCreate = conStrForCreate
        self.connectionString = connectionString
        self.db1 = DbClient(conStrForCreate)
        self.db2 = DbClient(connectionString)
        self.vk = Vk(token,connectionString)

    def menu(self):
        print("""Выберите необходимое действие:\n1-Создать базу данных для работы.(createDb)\n2-Создать необходимые таблицы.(createTables)\n3-Произвести поиск постов.(searchPosts)\n4-Поиск комментариев к постам\n""")
        choice = str(input())
        if choice=="createDb" or choice=="1":
            self.db1.createDb(self.conStrForCreate)
        if choice=="createTables" or choice=="2":
            self.db2.createTables()
        if choice=="searchPosts" or choice=="3":
            self.vk.searchPosts(self.id)
        if choice=="searchComments" or choice=="4":
            self.vk.searchComments(self.id)

if __name__=="__main__":
    id = '-135275127' # плейлистик дня
    token = 'YOUR_TOKEN'
    conStrForCreate = "Driver={SQL Server Native Client 11.0};Server=YOUR_SERVER_NAME;Database=master;Trusted_Connection=yes;"#строка подключения к БД для создания новой БД для работы
    connectionString = "Driver={SQL Server Native Client 11.0};Server=YOUR_SERVER_NAME;Database=vkontakte;Trusted_Connection=yes;"#строка подключения к используемой БД
    m = Main(id,token,conStrForCreate,connectionString)
    while(True):
        m.menu()