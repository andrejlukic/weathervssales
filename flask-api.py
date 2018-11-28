'''
Created on 23.11.2018

@author: test
'''
import os
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for
from werkzeug import secure_filename
import numpy as np
import pandas as pd
from gsod import GSOD
import datetime
import json

from bokeh.embed import json_item
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.sampledata.iris import flowers
from wutils import inplace_change
import hashlib

UPLOAD_FOLDER = 'data_sales/'
ALLOWED_EXTENSIONS = set(['txt','csv'])
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

colormap = {'setosa': 'red', 'versicolor': 'green', 'virginica': 'blue'}
colors = [colormap[x] for x in flowers['species']]

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
        
        hash_object = hashlib.md5(year.encode()+weather_station.encode()+file.filename.encode())
        print(hash_object.hexdigest())
        hash=hash_object.hexdigest()
        new_file = 'weathersalesresult-{}.html'.format(hash)
        new_file_path = 'templates\\'+new_file
        if(Path(new_file_path).is_file()):
            print('skip processing, return cached plot')
            return render_template(new_file)
        
        data_sales='data_sales'
        greader = GSOD()        
        dfweather = greader.getData(weather_station, int(year), int(year))
        print(dfweather.shape)
        dfsales = pd.read_csv(data_sales+r'\sales.csv')
        dfcategories = pd.read_csv(data_sales+r'\categories.csv').drop_duplicates('LowCategory')
        dfsales = pd.merge(dfsales,dfcategories,on='LowCategory')
        dfsales.columns=dfsales.columns.str.lower()
        data = pd.merge(dfweather, dfsales, on='dayofyear')
        
        
        
        inplace_change('templates/weathersalesresult.html', new_file_path, '#wsid#', weather_station)
        inplace_change(new_file_path, new_file_path, '#yr#', year)
        inplace_change(new_file_path, new_file_path, '#sf#', file.filename)
        
        # print(data['prcp'].corr(data['countpurchases']))
        return render_template(new_file)
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
@app.route('/hi')
def index2():
    return render_template('hello.html')
    # return "Hello, World!"

@app.route('/plot')
def plot():
    print('makin plot 1')
    
    data_sales='data_sales'
    greader = GSOD()        
    year = request.args.get('yr')
    wsid = request.args.get('wsid')
    sf = request.args.get('sf')
    cat = request.args.get('cat')
     
    if(not year):
        print('set year default 2018')
        year = 2018
    if(not wsid):
        print('set wsid default 140140')
        wsid = 140140
    if(not sf):
        print('set sf default sales.csv')
        sf = 'sales.csv'
    if(not cat):
        print('set cat default categories.csv')
        cat = 'categories.csv'
    dfweather = greader.getData(wsid, int(year), int(year))
    print(dfweather.shape)
    dfsales = pd.read_csv(data_sales+'\\'+sf)
    dfcategories = pd.read_csv(data_sales+'\\'+cat).drop_duplicates('LowCategory')
    dfsales = pd.merge(dfsales,dfcategories,on='LowCategory')
    dfsales.columns=dfsales.columns.str.lower()
    data = pd.merge(dfweather, dfsales, on='dayofyear')
        
    p = make_plot(data, 'petal_width', 'petal_length')
    return json.dumps(json_item(p, "myplot"))

#===============================================================================
# @app.route('/plot2')
# def plot2():
#     print('makin plot 2')
#     p = make_plot(None, 'petal_width', 'petal_length')
#     return json.dumps(json_item(p, "myplot"))
#===============================================================================

def make_plot(data, x, y):
    p = figure(title = "weather vs sales", sizing_mode="fixed", plot_width=400, plot_height=400)
    p.xaxis.axis_label = x
    p.yaxis.axis_label = y
    p.circle(data['prcp'], data['countpurchases'], color=colors, fill_alpha=0.2, size=10)
    return p

def make_example_plot(x, y):
    p = figure(title = "Iris Morphology", sizing_mode="fixed", plot_width=400, plot_height=400)
    p.xaxis.axis_label = x
    p.yaxis.axis_label = y
    p.circle(flowers[x], flowers[y], color=colors, fill_alpha=0.2, size=10)
    return p

@app.route('/bokeh_example')
def root():
    return render_template('bookeh_example.html')
    #return page.render(resources=CDN.render())

@app.route('/example_plot')
def example_plot():
    print('makin plot')
    p = make_example_plot('petal_width', 'petal_length')
    return json.dumps(json_item(p, "myplot"))

@app.route('/example_plot2')
def example_plot2():
    p = make_example_plot('sepal_width', 'sepal_length')
    return json.dumps(json_item(p))

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=80,debug=True)