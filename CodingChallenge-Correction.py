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


# Parse the attached Json and create a CSV file
def json_to_csv(file):
    """
    :param file: filepath
    :return: dataframe
    """
    # load json object
    try:
        data = []
        with open(file) as f:
            for line in f:
                data.append(json.loads(line))
        dic_flattened = [flatten(d) for d in data]
        df = pd.DataFrame(dic_flattened)
        df.drop('children', inplace=True, axis=1)

        # Create the parsed csv
        df.to_csv(r'parsed_csv.csv', index=False, header=True)
    except ValueError as e:
        print('Error {0}', e)
    return df

# Q2a Get only the maximum rating for an industry
def max_industry(dataframe):
    """
    :param dataframe:
    :return: Industry with max ratings
    Output:
    For e.g.
	Manufacturing     250
	Engineering       360
	Energy/Resources  480
    etc
    """
    industry_ls, rating_ls = [], []
    industry_ls.extend(dataframe['industry'].tolist())
    rating_ls.extend(dataframe['rating'].tolist())
    try:
        for column in dataframe.columns:
            if column.endswith('_industry'):
                industry_ls.extend(dataframe[column].tolist())
            elif column.endswith('_rating'):
                rating_ls.extend(dataframe[column].tolist())

        dataframe_new = pd.DataFrame.from_records(zip_longest(industry_ls, rating_ls), columns=['industry', 'rating'])
    # print(tabulate(dataframe_new, headers='keys', tablefmt='psql'))

        f_result = dataframe_new.groupby('industry').max()
    except ValueError as e:
        print('Error {0}', e)

    return f_result


# Q2b Get the minimum rating of a child company for the parent company
def transp_df_min(dataframe):

    """
    Output Sample:
    For eg:
        Parent_1    230
            Parent_1_child  150
                Parent_1_child_child 120
                    Parent_1_child_child_child 100

    Logic to iterate through parent and their child
    Filter the columns ending with rating
    Group by on t
    test=dataframe.filter(regex='rating$').head()
    print(test.T)
    test=test[test['rating'] == test.groupby('rating').transform('min')]
    #res1=res_t.groupby('rating').agg({'0':['min']})
    """
    try:
        cols = ['rating']
        for col in dataframe.columns:
            if col.startswith('children') and col.endswith('rating'):
                cols.append(col)
        res_t = dataframe[cols]
        #Transpose the dataframe
        res=res_t.T
    except ValueError as e:
        print('Error {0}', e)
    return res

#Q3c Transpose all the child companies as columns for a parent company?
def transpose_child_for_parent(dataframe):
    """
    Solution 1: Melt (Specifying the columns)

    Solution 2: set_index + stack

    :param dataframe:
    :return: transpose child with parent data

    Output:
    Parent_1  Parent_1_child  Parent_1_Child_Child Parent_1_child_child_child
    Guid
    Name
    rating
    Industrty
    etc

    """
    try:
     # df=dataframe.set_index('name').T
     df = dataframe.filter(regex='name$',axis=1)
      #res=dataframe.T
    except ValueError as e:
        print('Error {0}', e)
    return df


def main():

   # Hard Coded Value can be replaced with input parameters, renamed the file to test
    df_out = json_to_csv('test')
    # print(tabulate(df_out, headers='keys', tablefmt='psql'))

    # Question 2a: Get only the maximum rating for an industry
    print("Answer 2a: Get only the maximum rating for an industry")
    max_val = max_industry(df_out)
    print(tabulate(max_val, headers='keys', tablefmt='psql'))

    #Question 2b Get the minimum rating of a child company for the parent company
    print("Answer 2b: Get the minimum rating of a child company for the parent company")
    df_t = transp_df_min(df_out)
    print(tabulate(df_t, headers='keys', tablefmt='psql'))

    #Question 2c Transpose all the child companies as columns for a parent company?
    print("Answer 2c: Transpose all the child companies as columns for a parent company")
    df_child_to_parent=transpose_child_for_parent(df_out)
    print(tabulate(df_child_to_parent, headers='keys', tablefmt='psql'))

if __name__ == "__main__":
    main()









