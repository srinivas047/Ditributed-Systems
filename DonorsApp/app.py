from distutils.log import debug
import mysql.connector
import json
from flask import Flask, request, render_template, redirect, flash
import yaml

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
db = yaml.full_load((open('db.yaml')))

@app.route('/',methods=('GET', 'POST'))
def hello_world():
  if request.method == 'POST':

    name = request.form['name']
    email = request.form['email']
    amount = request.form['amount']

    mydb = mysql.connector.connect(
      host = db['mysql_host'],
      user = db['mysql_username'],
      password = db['mysql_password']
      )
    cursor = mydb.cursor()

    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()

    if (db['mysql_db'], ) not in databases:
      cursor.execute("CREATE DATABASE DB_Phase1")
      cursor.close()

    mydb = mysql.connector.connect(
      host = db['mysql_host'],
      user = db['mysql_username'],
      password = db['mysql_password'],
      database = db['mysql_db']
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
    return redirect('/donors')
  
  return render_template('index.html')

@app.route("/donors")
def donors():
    mydb = mysql.connector.connect(
    host = db['mysql_host'],
    user = db['mysql_username'],
    password = db['mysql_password'],
    database = db['mysql_db']
    )
    cursor = mydb.cursor()

    cursor.execute("SELECT * FROM donors")

    results = cursor.fetchall()
    flash('Login successful')
    return render_template('donors.html', userDetails=results)

if __name__ == "__main__":
  app.run(host ='0.0.0.0', debug=True)
