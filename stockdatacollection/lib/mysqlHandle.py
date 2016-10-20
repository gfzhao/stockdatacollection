import os,sys
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def getProperties(fileName):   
   try:
      pro_file = open(fileName, 'r')
      properties = {}
      for line in pro_file:
         if line.find('=') > 0:
            strs = line.replace('\n', '').split('=')
            properties[strs[0].strip()] = strs[1].strip()
   except Exception, e:
      raise e
   else:
      pro_file.close()
      return properties

class MysqlHandle:
   fileName = os.path.join(os.getenv("PYTHONPATH"),'conf','mysql.properties')
   engine = None

   def __init__(self):
     self.session = self.__create_db_session()

   @staticmethod
   def create_db_engine():
      properties = getProperties(MysqlHandle.fileName)
      dbType = properties['databaseType']
      dbUser = properties['databaseUser']
      dbPasswd = properties['databasePasswd']
      dbIP = properties['databaseIP']
      dbName = properties['databaseName']
      databaseConStr = ''
      databaseConStr = databaseConStr + dbType
      databaseConStr = databaseConStr + '://' + dbUser
      databaseConStr = databaseConStr + ':' + dbPasswd
      databaseConStr = databaseConStr + '@' + dbIP
      databaseConStr = databaseConStr + '/' + dbName
      databaseConStr = databaseConStr + '?charset=utf8'
      if MysqlHandle.engine == None:
         MysqlHandle.engine = create_engine(databaseConStr,echo=True)

   def __create_db_session(self):
      Session = sessionmaker(bind=MysqlHandle.engine)
      return Session()

MysqlHandle.create_db_engine()
'''
Example:

fileName = sys.path[0] + '\\'+ 'system.properties'
p = Properties(fileName)
properties = p.getProperties()
print properties[Key
'''