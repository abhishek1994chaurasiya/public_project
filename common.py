from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import errorcode

class Common(object):
    def CreateSparkSession(self,mappingName):
			# from pyspark.sql import SparkSession
			# Build spark Session instance to interact with Spark Core
			spark = SparkSession.builder.appName(mappingName).getOrCreate();
			return spark;
    def mysql_connector(self,query,strategy):
        config = {
            'user' : <<user_name>>,
            'password' : <<password>>,
            'host' : <<ip_address>>,
            'database' : <<mysql_DB>>,
            'charset' : 'utf8mb4',
            'raise_on_warnings' : True
        }
        db = mysql.connector.connect(**config)
        cursor = db.cursor()
        cursor.execute(query)

        if (strategy.upper() == "SELECT"):
            result=cursor.fetchall()
            cursor.close()
            db.close()
            return result
        else:
            db.commit()
            cursor.close()
            db.close()
            return