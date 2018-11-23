'''
Created on 23.11.2018

@author: test
'''
import os
from flask import Flask, render_template, request, redirect, url_for
from werkzeug import secure_filename
import numpy as np
import pandas as pd
from gsod import GSOD
import datetime

UPLOAD_FOLDER = 'data_sales/'
ALLOWED_EXTENSIONS = set(['txt','csv'])
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS
@app.route("/upload", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        weather_station = request.form.get('weather_station')
        year = request.form.get('year')
        print('Debug-Form values: {}-{}'.format(weather_station,year))
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            print(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            #return redirect(url_for('index'))
        
        data_sales='data_sales'
        greader = GSOD()
        
        dfweather = greader.getData(weather_station, int(year), int(year))
        print(dfweather.shape)
        dfsales = pd.read_csv(data_sales+r'\sales.csv')
        dfcategories = pd.read_csv(data_sales+r'\categories.csv').drop_duplicates('LowCategory')
        dfsales = pd.merge(dfsales,dfcategories,on='LowCategory')
        dfsales.columns=dfsales.columns.str.lower()
        data = pd.merge(dfweather, dfsales, on='dayofyear')
        # print(data['prcp'].corr(data['countpurchases']))
    return render_template('weathersalesuploadform.html')
    #===========================================================================
    # return """
    # <!doctype html>
    # <title>upload data</title>
    # <h1>upload tx list</h1>
    # <form action="" method=post enctype=multipart/form-data>
    #   <p><input type=file name=file>
    #      <input type=submit value=Upload>
    # </form>
    # <p>%s</p>
    # """ % "<br>".join(os.listdir(app.config['UPLOAD_FOLDER'],))
    #===========================================================================
@app.route('/')
def index2():
    return render_template('hello.html')
    # return "Hello, World!"
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=80,debug=True)