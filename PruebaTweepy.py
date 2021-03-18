import tweepy
from numpy import *
import csv
import cx_Oracle
import os
from datetime import datetime

#Función para extraer ciertos caracteres de una cadena.
def mid(s, offset, amount):
    return s[offset:offset+amount]

#Función para convertir mes en número.	
def mesANumero(shortMonth):

	return{
		"Jan" : 1,
		"Feb" : 2,
		"Mar" : 3,
		"Apr" : 4,
		"May" : 5,
		"Jun" : 6,
		"Jul" : 7,
		"Aug" : 8,
		"Sep" : 9,
		"Oct" : 10,
		"Nov" : 11,
		"Dec" : 12
	}[shortMonth]

#Conexión a Base de Datos.
ip = '170.70.130.227'
port = 1521
SID = 'DATAWHIE'
os.environ["NLS_LANG"] = ".UTF8"
dsn_tns = cx_Oracle.makedsn(ip, port, SID)
con = cx_Oracle.connect('ETC_BIGDGIE','8c7KQ6MD92pyeS42Pu9n3X7ZBh9g5n',dsn_tns)
cur = con.cursor()
errores_usr = 0
errores_tweet = 0

#Creación de archivo de soporte para descarga.
csvFile = open('PruebaTwitter.csv', 'a', encoding = 'utf-8')
csvWriter = csv.writer(csvFile)

#Creación de variables con las llaves que proporciona Twitter.
consumer_key = "tfZckCldByowUbK2H47NtywuY"
consumer_secret = "pukD0n1MadMdUE0txhfQ0szsckfv9QzdMVGm59wvEU6T0h89lC"
access_token_key = "1650568256-zDl2ag2M4tSUfROtpdPbX5w5obdajEbw2KH2PEA"
access_token_secret = "wJGha83fYFUs1aibvvoqIg4djVQqzNEQS74gomhl4i70V"

#Creación de Request con las llaves del entorno de desarrollo.
auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token_key,access_token_secret)

x = tweepy.API(auth)

#Iteración de la respuesta generada por Twitter.
for tweets in x.search(q = 'desabasto', count = 100, rpp = 100):

		 #result_type ='popular'
	#print(row)
	try:
		#Separación de fecha por día, mes y año.
		print(tweets.user.created_at)
		dateusr_dia = tweets.user.created_at.strftime("%d");
		dateusr_mes = tweets.user.created_at.strftime("%b");
		dateusr_año = tweets.user.created_at.strftime("%Y");
		#Consulta para obtener la llave tiempo del registro descargado.
		fechausr = (dateusr_dia, mesANumero(dateusr_mes), dateusr_año)
		cur.execute("SELECT LLAVE_TIEMPO FROM DATAWH_BIGDGIE.DGIE_TIEMPO WHERE DIA = :1 AND MES = :2 AND ANIO = :3", fechausr)
		llave_tiempo_usr = cur.fetchone()
		#Obtención e inserción de la información del usuario en tabla "TWITTER_DATOS_USUARIOS".
		row_usr = (tweets.user.id, llave_tiempo_usr[0], tweets.user.screen_name,tweets.user.friends_count, tweets.user.favourites_count, tweets.user.created_at,tweets.user.lang, tweets.user.followers_count, tweets.user.time_zone, tweets.user.statuses_count, tweets.user.following, tweets.user.name, tweets.user.location)
		print("Insertado informacion de usuario...")
		cur.execute("INSERT INTO DATAWH_BIGDGIE.TWITTER_DATOS_USUARIOS (ID_USUARIO, LLAVE_TIEMPO, NOMBRE_USUARIO, FRIENDS_COUNT, FAVOURITE_COUNT, FECHA_CREACION_USUARIO, IDIOMA, NUMERO_SEGUIDORES, TIEMPO_HORARIO, NUMERO_TWEETS, ISFOLLOWING, NOMBRE, UBICACION) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)", row_usr)
		con.commit()
	except:
		errores_usr = errores_usr + 1
	try:
		#Separación de fecha por día, mes y año.
		datestr_dia = tweets.created_at.strftime("%d")
		datestr_mes = tweets.created_at.strftime("%b")
		datestr_año = tweets.created_at.strftime("%Y")
		#Consulta para obtener la llave tiempo del registro descargado.
		fecha = (datestr_dia, mesANumero(datestr_mes), datestr_año)
		cur.execute("SELECT LLAVE_TIEMPO FROM DATAWH_BIGDGIE.DGIE_TIEMPO WHERE DIA = :1 AND MES = :2 AND ANIO = :3", fecha)
		llave_tiempo = cur.fetchone()
		#Obtención e inserción de la información del usuario en tabla "TWITTER_DATOS_USUARIOS".
		row_tweet = (tweets.id, llave_tiempo[0], tweets.user.id, tweets.created_at, tweets.retweet_count, tweets.in_reply_to_status_id, tweets.in_reply_to_user_id, tweets.in_reply_to_screen_name, tweets.retweeted, tweets.text, tweets.place, tweets.lang)
		print("Insertando informacion de Tweets...")
		cur.execute("INSERT INTO DATAWH_BIGDGIE.TWITTER_DATOS_TWEET(ID_TWEET, LLAVE_TIEMPO, ID_USUARIO, FECHA_CREACION, NUMERO_RETWEETS, REPLY_STATUS_ID, REPLY_USER_ID, REPLY_NAME, IS_RETWEETED, MENSAJE, LUGAR, IDIOMA) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)", row_tweet)
		con.commit()
	except:
		#El manejo de excepción de errores de da cuando un tweet ya ha sido registrado en la base de datos evitando duplicidad.
		errores_tweet = errores_tweet + 1
con.close()