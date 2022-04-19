from distutils.log import debug
import mysql.connector
import json
from flask import Flask, request, render_template, redirect, url_for
import yaml
import os
import requests
import pandas as pd

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
db = yaml.full_load((open('db.yaml')))

@app.route('/',methods=('GET', 'POST'))
def hello_world():
  if request.method == 'POST':

    name = request.form['name']
    email = request.form['email']
    amount = request.form['amount']

    if os.environ["IS_LEADER"]=="true": 
      final_outputs = []     
      x = requests.post("http://server_2:5000/", {'name':name, 'email': email, 'amount': amount})
      x = requests.post("http://server_3:5000/", {'name':name, 'email': email, 'amount': amount})
      Populate_database(db['host1'], '3306', name, email, amount)

    elif request.host_url == "http://server_2:5000/":
      Populate_database(db['host2'], '3307', name, email, amount)

    elif request.host_url == "http://server_3:5000/":
      Populate_database(db['host3'], '3308', name, email, amount)
    
    return redirect('/leaders')
  
  return render_template('index.html')

def Populate_database(host, port, name, email, amount):
    mydb = mysql.connector.connect(
      host = host,
      user = db['mysql_username'],
      password = db['mysql_password'],
      port = port
      )

    cursor = mydb.cursor()

    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()

    if (db['mysql_db'], ) not in databases:
      cursor.execute("CREATE DATABASE DB_Phase2")
      cursor.close()

    mydb = mysql.connector.connect(
      host = host,
      user = db['mysql_username'],
      password = db['mysql_password'],
      database = db['mysql_db'],
      port = port
      )
    cursor = mydb.cursor()

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    table = "donors"

    if (table, ) not in tables:
      cursor.execute("CREATE TABLE donors (name VARCHAR(40), email VARCHAR(40), amount INT(20)) ")

    sql = "INSERT INTO donors (name, email, amount) VALUES (%s, %s, %s)"
    val = (name, email, amount)
    cursor.execute(sql, val)
    mydb.commit()
    cursor.close()

def Retrive_results_DB(host, port):

  mydb = mysql.connector.connect(
    host = host,
    user = db['mysql_username'],
    password = db['mysql_password'],
    database = db['mysql_db'],
    port = port
    )
  cursor = mydb.cursor()

  cursor.execute("SELECT * FROM donors")

  results = cursor.fetchall()

  return results


@app.route("/leaders")
def leaders():

  if os.environ["IS_LEADER"]=="true":
    results = Retrive_results_DB(db['host1'], '3306')

  elif request.host_url == 'http://localhost:8001/':
    results = Retrive_results_DB(db['host2'], '3307')

  elif request.host_url == 'http://localhost:8002/':
    results = Retrive_results_DB(db['host3'], '3308')

  return render_template('donors.html', userDetails1=results)

if __name__ == "__main__":
  app.run()
