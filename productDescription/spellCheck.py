# -*- coding: utf-8 -*-

import requests
import psycopg2
import pandas as pd
import numpy as np
import datetime
from datetime import datetime as dt
import language_tool_python
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from fuzzysearch import find_near_matches
import statistics 


class postgres_conn:
    def getConn(self):
        try:
            connection = psycopg2.connect(user='postgres',
                                          password='postgres_007',
                                          host="1.pgsql.db.1digitalstack.com",
                                          port='5432',
                                          database='postgres')

            cursor = connection.cursor()
            # Print PostgreSQL Connection properties
            print(connection.get_dsn_parameters(), "\n")

            # Print PostgreSQL version
            return cursor, connection

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            return error, error

    def close_connection(self, cursor, connection):
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

class SpellCheck:
    # get data
    def __init__(self):
        self.tool = language_tool_python.LanguageTool('en-US')
    # query_readyprod = f""" 

    #     Select distinct a.channel_sku_id,a.sku_title,b.keyword_id,c.name,d.search_volume from entity.product_feature_mapping a 
    #     left join client_resource.client_keyword_mapping b on a.channel_sku_id = b.sku_id left join entity.keyword_master c
    #     on b.keyword_id = c.id left join (Select searchterm, avg(search_volume) as search_volume from ams.ams_seller_search where 
    #     EXTRACT(MONTH FROM input_period_start_day) = 11 and EXTRACT(Year FROM input_period_start_day)= 2020 
    #     Group By searchterm) d on lower(c.name) = d.searchterm 
    #     where a.brand='{brand}' 
    #     order by a.channel_sku_id,b.keyword_id"""
    def getData(self,brand):
            
        pg = postgres_conn()
        conn = pg.getConn()

        query_readyprod = f""" 

          Select distinct a.channel_sku_id,a.sku_title,b.keyword_id,c.name,d.search_volume from entity.product_feature_mapping a 
          left join client_resource.client_keyword_mapping b on a.channel_sku_id = b.sku_id left join entity.keyword_master c
          on b.keyword_id = c.id left join (Select searchterm, avg(search_volume) as search_volume from ams.ams_seller_search where 
          EXTRACT(MONTH FROM input_period_start_day) = 11 and EXTRACT(Year FROM input_period_start_day)= 2020 
          Group By searchterm) d on lower(c.name) = d.searchterm 
          where a.brand='{brand}' 
          order by a.channel_sku_id,b.keyword_id"""

        

        df_readyprod = pd.read_sql_query(query_readyprod, conn[1])
        return df_readyprod

    ##################################################################


    # Word Count ########################################################################

    def title_word_count(self,df,col):
        return df[col].str.len()

    

    # Word Duplicate Count ########################################################################

    def title_word_duplicate_count(self,df,title,duplicateTitleCount):
       
        df[duplicateTitleCount] = np.nan
        for i in range(len(df[title])):
           df[duplicateTitleCount][i] = len(df[title][i].split()) - len(set(df[title][i].split()))
        print(duplicateTitleCount+ " column added to df")
        

    

    # Spell Checker ########################################################################

    

    def title_spell_check_df(self,df,title,errorList):
        df[errorList] = np.nan
        for i in range(len(df[title])):
            ith_title =  df[title][i]
            df[errorList][i] = title_spell_check(ith_title,self.tool)
    
        
                 

   #  check for description formatting#############

    


    def fuzzy_extract_df(self,df,sku_title,keyword):
        df["Match_Keyword"] = np.nan
        df["Match_Score"] = np.nan
        for ind in df.index:
            title = df[sku_title][ind]
            key = df[keyword][ind]
            if title is not None and key is not None :
                x,y = fuzzy_extract(key, title)
                df["Match_Keyword"][ind] = x
                df["Match_Score"][ind] = y
            else:
                df["Match_Keyword"][ind] = None
                df["Match_Score"][ind] = None

        df["Weighted"] = df.apply(lambda row:(row['search_volume']*row['Match_Score']),axis=1)

    def getWeightedMatchScore(self,df,asin,title,search_volume,weighted):
        df_agg = df.groupby([asin,title]).agg(
        Sum_search = pd.NamedAgg(column = search_volume, aggfunc = sum),
        Sum_weighted = pd.NamedAgg(column = weighted, aggfunc = sum)
        )

        df_agg["Match_Score_Weighted"] = df_agg.apply(lambda row:(row['Sum_weighted']/row['Sum_search']),axis=1)
        return df_agg

#################################################################################################################

def title_spell_check(title,tool):
    list_error = []
    matches = tool.check(title)
    for i in matches:
        if i.ruleIssueType == 'misspelling':
            list_error.append(title[i.offset:i.offset+i.errorLength])
    return list_error 


def fuzzy_extract(keyword,title):  
    keyword = keyword.lower()
    title = title.lower()
    kw = keyword.split(" ")
    
    count = len(kw)
    
    s2 = 0
    kwd2 = ""
    
    for k in kw:
        s1 = 0
        kwd1 = ""
        for match in find_near_matches(k, title, max_l_dist=1):
            match = match.matched
            index = fuzz.WRatio(match, k)
            if index>70 and index>s1:
                kwd1 = match
                s1 = index
                
        kwd2 = kwd2 + " " + kwd1
        if s2 == 0:
            s2 = s1
        else:
            s2 = (s2+s1)
    return (kwd2,s2/count)

