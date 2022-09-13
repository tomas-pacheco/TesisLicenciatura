# -*- coding: utf-8 -*-
"""
Created on Sat Aug 27 01:04:35 2022

@author: Tomas
"""

## Para correr hoy

# Abro las librerias que necesito
import urllib.request
from pprint import pprint
from html_table_parser.parser import HTMLTableParser
import pandas as pd
import os
import numpy as np
import time


os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\\output\\downloads')
#os.chdir('/Volumes/GoogleDrive/Mi unidad/UdeSA/Tesis')

#fac = pd.read_excel (r'G:\\Mi unidad\\UdeSA\\Tesis\\repeccode.xlsx',
#                    sheet_name='Sheet1')

fac = pd.read_excel (r'G:\\Mi unidad\\UdeSA\\Tesis\\input\\scraping\\repeccode.xlsx',
                    sheet_name='Sheet1')

universities_list = list(np.unique(np.array(fac.iloc[:,0])))
 

# Defino una funcion que baja las tablas 
 
def url_get_contents(url):
    #making request to the website
    req = urllib.request.Request(url=url)
    f = urllib.request.urlopen(req)
    #reading contents of the website
    return f.read()

start = time.time()
count = 1
for univ in universities_list:
    univ_sub = fac[fac['university'] == univ]
    codes_u = list(univ_sub.iloc[:,2])
    
    df = pd.DataFrame(['Paper', 'Last month','3 months', '12 months', 'Total', 
                         'Last month', '3 months', '12 months', 'Total']).T
    ja = codes_u[0:10]
    for author in ja:
        try:
            xhtml = url_get_contents('https://logec.repec.org/RAS/{}.htm'.format(author)).decode('utf-8') # defining the html contents of a URL.
            p = HTMLTableParser() # Defining the HTMLTableParser object
            p.feed(xhtml) # feedin

            for r in range(0,len(p.tables)-1): #itero a traves de las tablas de cada autor
                t = pd.DataFrame(p.tables[r])
                t = pd.DataFrame(t.iloc[-1,:]).T
                #t = t.iloc[2:len(t)]
                t['Code'] = author
                df = df.append(t, ignore_index=True)
    
        except:
            pass
        
    df['univ'] = univ
    df.to_csv('{}.csv'.format(univ), index=False) 
    
    print('Van {} de 180, o sea {}%'.format(count, round(count/180*100,2)))
    count += 1
    
end = time.time()
time_downloads = end - start      
    
# Exportamos un csv por universidad. Ahora los vamos a juntar en un solo csv

os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\\output\\downloads')

df_all = pd.DataFrame()

for file in universities_list:
    df_temp = pd.read_csv ('{}.csv'.format(file))
    df_all = df_all.append(df_temp)

os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\\output')
df_all.to_csv('full_visits.csv', index=False) 



#############################################################################3
#############################################################################3
#############################################################################3
#############################################################################3



# Importo librerias 

import pandas as pd
import os
import numpy as np
from bs4 import BeautifulSoup
import requests
import re
import time

# Defino todas las funciones que voy a usar

def research_experience(datos):
    l = len(datos)
    
    for r in range(0,l):
        cond = 'years' in str(datos[r])
        
        if cond == True:
            index_authors = r
        else:
            continue
        
    return str(re.split('\\xa0\\xa0', str(re.split('years', str(datos[index_authors]))[0]))[-1])


def cantidad_coauthors(datos):
    l = len(datos)
    
    for r in range(0,l):
        cond = 'Works with' in str(datos[r]) 
        if cond == True:
            index_authors = r
        else:
            continue
            
    return int(str(datos[index_authors]).count('</a>'))
      
    
     
def research_production(bsoup):

    data_pub = bsoup.findAll('p',attrs= {'class':'mainAutDatC'})
    data_quant = bsoup.findAll('p',attrs= {'class':'mainAutDatN'})
    
    articles = 0
    books = 0 
    papers = 0

    for k in range(0, len(data_pub), 1):
        
        if 'Articles' in data_pub[k]:
            articles = re.sub("[^0-9]", "", str(data_quant[k]))
        elif 'Papers' in data_pub[k]:
            papers = re.sub("[^0-9]", "", str(data_quant[k]))
        elif 'Books' in data_pub[k]:
            books = re.sub("[^0-9]", "", str(data_quant[k]))
        else: 
            None

    return articles, papers, books


# Defino directorio:

os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\\output\\citations')
#os.chdir('/Volumes/GoogleDrive/Mi unidad/UdeSA/Tesis/output')

fac = pd.read_excel (r'G:\\Mi unidad\\UdeSA\\Tesis\\input\\scraping\\repeccode.xlsx',
                    sheet_name='Sheet1')

#fac = pd.read_excel (r'/Volumes/GoogleDrive/Mi unidad/UdeSA/Tesis/repeccode.xlsx',
#                  sheet_name='Sheet1')

universities_list = list(np.unique(np.array(fac.iloc[:,0])))
 
start = time.time()
count = 1
for univ in universities_list:
    univ_sub = fac[fac['university'] == univ]
    codes_u = list(univ_sub.iloc[:,2])
    surname_u = list(univ_sub.iloc[:,3])
    
    df = pd.DataFrame(index=np.arange(len(surname_u)), 
                      columns = ['author_code', 'hindex', 'i10index', 'citations', 
                                 'years', 'articles', 'papers' ,'books' ,'coauthors'])
    

    for indexp in range(0,len(surname_u),1):
        
        df.iloc[indexp,0] = codes_u[indexp]
        
        try:
            page = requests.get('http://citec.repec.org/p/{}/{}.html'.format(surname_u[indexp].lower(),codes_u[indexp]))
            soup = BeautifulSoup(page.content, "html.parser")
            

            # Busco informacion sobre hindex, i10index y citations

            data1 = soup.findAll('p',attrs={'class':'indData'})
            row = 1
            for e in data1:
                df.iloc[indexp, row] = re.split('>', str(re.split('</p>', str(e))[0]))[-1]
                row += 1                
            
            # Years de research
            
            data2 = soup.findAll('div',attrs={'id':'mainAutDat'})
            df.iloc[indexp,4] = research_experience(data2)

            # Cantidad de articulos, papers y libros

            res = research_production(soup)
            df.iloc[indexp,5] = res[0]
            df.iloc[indexp,6] = res[1]
            df.iloc[indexp,7] = res[2]

            # Cantidad de coautores
            
            data4 = soup.findAll('td',attrs={'class':'headerNumbers'})
            df.iloc[indexp,8] = cantidad_coauthors(data4)
            
        except:
            pass
        
        
    df['univ'] = univ
    df.to_csv('{}.csv'.format(univ), index=False) 
    
    print('Van {} de 180, o sea {}%'.format(count, round(count/180*100,2)))
    count += 1
end = time.time()

time_citations = end - start
    
 
# Exporto en un solo dataframe

os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\\output\\citations')

df_all = pd.DataFrame()

for file in universities_list:
    df_temp = pd.read_csv ('{}.csv'.format(file))
    df_all = df_all.append(df_temp)
    
#os.chdir('/Volumes/GoogleDrive/Mi unidad/UdeSA/Tesis/output') 
os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\\output')

df_all.to_csv('full_citations.csv', index=False) 

    
