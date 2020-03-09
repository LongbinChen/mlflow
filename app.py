from flask import Flask, render_template
from flask_debugtoolbar import DebugToolbarExtension
from flask import Flask, redirect, url_for, render_template, request, session
from flask import jsonify

import json
import sys
import os
from os import listdir
from dag2rete import dag_to_rete_json


app = Flask(__name__)

app.debug = True
app.secret_key = 'development key'

@app.route('/', methods=['GET', 'POST'])
def login():
    print(listdir("sample"))
    return render_template('index.html', files = listdir("sample"))

@app.route("/load/", methods=['POST'])
def load():
    print(str(request.form['name']))
    with open(os.path.join("sample",request.form['name'])) as file:
        if request.form['name'].endswith(".txt"):
            d = json.load(file)
        else:
            d = dag_to_rete_json(file)
        return jsonify(d)
	
@app.route("/save/", methods=['POST'])
def save():
    with open(os.path.join("sample",request.form['name']+".txt"),"w") as file:  
        file.write(request.form['editor'])
        print(request.form['editor'])
    return ""


@app.route('/')
def hello():
    return "Hello World!"

@app.route('/demo')
def demo():
    return render_template("demo.html")

if __name__ == '__main__':
    app.run()
