#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug  6 00:07:40 2022

@author: tomaspacheco
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 22:34:28 2022

@author: tomaspacheco
"""


# Importo librerias 

import pandas as pd
import os
import numpy as np
from bs4 import BeautifulSoup
import requests
import re


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


# Defino directorio

#os.chdir('G:\\Mi unidad\\UdeSA\\Tesis\output')
os.chdir('/Volumes/GoogleDrive/Mi unidad/UdeSA/Tesis')

#fac = pd.read_excel (r'G:\\Mi unidad\\UdeSA\\Tesis\\repeccode.xlsx',
#                    sheet_name='Sheet1')

fac = pd.read_excel (r'/Volumes/GoogleDrive/Mi unidad/UdeSA/Tesis/repeccode.xlsx',
                    sheet_name='Sheet1')

universities_list = list(np.unique(np.array(fac.iloc[:,0])))
 

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
    
    print('Van {} de 117, o sea {}%'.format(count, round(count/117*100,2)))
    count += 1
    
    
 

         