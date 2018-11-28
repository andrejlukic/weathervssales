'''
Created on 21.11.2018

@author: Andrej Lukic
'''
import numpy as np
import pandas as pd
import datetime

def fahrenheitToCelzius(tempF):
    return (tempF - 32) * 5.0/9.0
    #return [ ((tf - 32) * 5.0/9.0) for tf in tempF]

def milesToKm(distanceMiles):
    conversion_factor = 0.62137119
    return distanceMiles / conversion_factor

def inchToCm(distanceInch):
    conversion_factor = 2.54
    return distanceInch * conversion_factor

def knotsToKm(knots):
    conversion_factor = 1.85200
    return knots * conversion_factor

def inplace_change(filename, newfilename, old_string, new_string):
    # Safely read the input filename using 'with'
    with open(filename) as f:
        s = f.read()
        
    # Safely write the changed content, if found in the file
    with open(newfilename, 'w') as f:        
        s = s.replace(old_string, new_string)
        f.write(s)
        
        
        
        