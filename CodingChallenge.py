import json
import pandas as pd 
import csv
from pandas.io.json import json_normalize
from functools import reduce 
from collections import ChainMap 
from flatten_json import flatten
import re
from tabulate import tabulate
from itertools import zip_longest

# Qa: Parse the attached Json and create a CSV file 
def json_to_csv(file):
    #load json object
    data = []
    with open(file) as f:
        for line in f:
            data.append(json.loads(line))

    dic_flattened = [flatten(d) for d in data]
    # print(dic_flattened)


    df = pd.DataFrame(dic_flattened)
    df.drop('children', inplace=True, axis=1)

    # Run it once then comment it
    df.to_csv (r'parsed_csv.csv', index = False, header=True)
    return df

# Qb(i): Get only the maximum rating for an industry
def max_industry(dataframe):
    industry_ls, rating_ls = [], []

    industry_ls.extend(dataframe['industry'].tolist())
    rating_ls.extend(dataframe['rating'].tolist())

    for column in dataframe.columns:

        if column.endswith('_industry'):
            industry_ls.extend(dataframe[column].tolist())

        elif column.endswith('_rating'):
            rating_ls.extend(dataframe[column].tolist())

    dataframe_new = pd.DataFrame.from_records(zip_longest(industry_ls, rating_ls), columns=['industry', 'rating'])
    #print(tabulate(dataframe_new, headers='keys', tablefmt='psql'))

    
    f_result = dataframe_new.groupby('industry').max()
    return f_result

#Qb(ii) -- You can go from here, you can further clean the dataset apply logics
def transp_df(dataframe):
    
    res_t = dataframe.T
    return res_t
    

def main():

    #Renamed the input file to test
    df_out = json_to_csv('test')
    #print(tabulate(df_out, headers='keys', tablefmt='psql'))
    
    max_val = max_industry(df_out)
    print(tabulate(max_val, headers='keys', tablefmt='psql'))
    
    df_t = transp_df(df_out)
    print(tabulate(df_t, headers='keys', tablefmt='psql'))


if __name__ == "__main__":
    main()



    

  

#print(rating_ls)
#print('------------/n')
#print(industry_ls)









