# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 16:54:34 2020

@author: DELL
"""
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
import PIL
from PIL import Image
import requests
from io import BytesIO 
import cv2
import urllib
import pytesseract



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
            

##############################################################

class ImgData:

    def getdata(self,brand):

        pg = postgres_conn()
        conn = pg.getConn()


        query_readyprod = f""" 
        Select distinct a.brand, a.channel_sku_id,b.aplus_images from entity.product_feature_mapping a left join 
        ready.ready_product_details b  on a.channel_sku_id = b.asin where a.brand = '{brand}' """

        df_readyprod = pd.read_sql_query(query_readyprod, conn[1])

        return df_readyprod

############################################################################################################################
    def getImageTxtPercent(self,df):
        errorImglist = {}
        for ind in range(len(df)):
            images = df.iloc[ind,:]['aplus_images']
            if(images!= None):
                for imageIx in range(len(images)):
                    try:
                        req = urllib.request.urlopen(images[imageIx])
                        arr = np.asarray(bytearray(req.read()), dtype=np.uint8)
                        image = cv2.imdecode(arr, -1) 
                        
                        
                        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                        thresh = cv2.adaptiveThreshold(gray,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV,11,3)
                    
                        mask = thresh.copy()
                        mask = cv2.merge([mask,mask,mask])

                        picture_threshold = image.shape[0] * image.shape[1] * .05
                        cnts = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
                        for c in cnts:
                            area = cv2.contourArea(c)
                            if area < picture_threshold:
                                cv2.drawContours(mask, [c], -1, (0,0,0), -1)

                        mask = cv2.cvtColor(mask, cv2.COLOR_BGR2GRAY)
                        result = cv2.bitwise_xor(thresh, mask)

                        text_pixels = cv2.countNonZero(result)
                        percentage = (text_pixels / (image.shape[0] * image.shape[1])) * 100
                        imageNum = 'image_' + str(imageIx) + 'textPercent'
                        if(imageNum not in df):
                            df[imageNum] = np.nan
                        df[imageNum][ind] = percentage
                        cv2.waitKey(1)
                        cv2.destroyAllWindows()
                    except Exception as e:
                        try:
                            if(df.iloc[ind,:]['channel_sku_id'] not in errorImglist):
                                errorImglist[df.iloc[ind,:]['channel_sku_id']] = list()

                            errorImglist[df.iloc[ind,:]['channel_sku_id']].append(images[imageIx])
                            print(errorImglist[df.iloc[ind,:]['channel_sku_id']])
                        except Exception as e1:
                            pass

        return errorImglist    

#####################################################################################################################
    def getImgTxt(self,df,tesseractPath):
        errorImglist = {}
        for ind in range(len(df)):
            images = df.iloc[ind,:]['aplus_images']
            if(images!= None):
                for imageIx in range(len(images)):
                    try:
                    
                        urllib.request.urlretrieve(images[imageIx], "sample.png")
                        img = Image.open('sample.png').convert('L')
                        ret,img = cv2.threshold(np.array(img), 125, 255, cv2.THRESH_BINARY)
                        img = Image.fromarray(img.astype(np.uint8))
                        pytesseract.pytesseract.tesseract_cmd = tesseractPath
                        text = pytesseract.image_to_string(img)
                                        
                        imageText = 'image_' + str(imageIx) + 'text'
                        if(imageText not in df):
                            df[imageText] = np.nan
                        df[imageText][ind] = text
                    
                    except Exception as e2:
                        try:
                            if(df.iloc[ind,:]['channel_sku_id'] not in errorImglist):
                                    errorImglist[df.iloc[ind,:]['channel_sku_id']] = list()

                            errorImglist[df.iloc[ind,:]['channel_sku_id']].append(images[imageIx])
                            print(errorImglist[df.iloc[ind,:]['channel_sku_id']])
                        except Exception as e3:
                            pass

        return errorImglist    







#############################################################################################################################

    # for j in df_readyprod.aplus_images:
    #     if j == None: continue
    #     for i in j:
    #         print(i)

    # response = requests.get(i)
    # imz = Image.open(BytesIO(response.content))

    # urllib.request.urlretrieve(Link4, "sample.png")
    # imzz = PIL.Image.open("sample.png")
    # imzz.show()

    # ############################### % Text in Image ######################################

    # image = cv2.imread('sample.png')
    # gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # thresh = cv2.adaptiveThreshold(gray,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV,11,3)

    # mask = thresh.copy()
    # mask = cv2.merge([mask,mask,mask])

    # picture_threshold = image.shape[0] * image.shape[1] * .05
    # cnts = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    # cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    # for c in cnts:
    #     area = cv2.contourArea(c)
    #     if area < picture_threshold:
    #         cv2.drawContours(mask, [c], -1, (0,0,0), -1)

    # mask = cv2.cvtColor(mask, cv2.COLOR_BGR2GRAY)
    # result = cv2.bitwise_xor(thresh, mask)

    # text_pixels = cv2.countNonZero(result)
    # percentage = (text_pixels / (image.shape[0] * image.shape[1])) * 100
    # print('Percentage: {:.2f}%'.format(percentage))

    # cv2.imshow('thresh', thresh)
    # cv2.imshow('result', result)
    # cv2.imshow('mask', mask)
    # cv2.waitKey()

    # ##################### Find Text #############
    # Link1 = 'https://m.media-amazon.com/images/S/aplus-media/vc/301a1e34-0f81-49fb-b0f8-2fdf92500733.__CR0,0,970,300_PT0_SX970_V1___.jpg'
    # Link2 = 'https://m.media-amazon.com/images/S/aplus-media/vc/c349422e-aa91-4619-90e4-e7650774abf2.__CR1,0,1917,593_PT0_SX970_V1___.jpg'
    # Link3 = 'https://m.media-amazon.com/images/S/aplus-media/vc/9445ae11-e127-4add-bc9d-339b26b1d900.__CR0,0,970,300_PT0_SX970_V1___.jpg'
    # Link4 = 'https://m.media-amazon.com/images/S/aplus-media/sota/71919180-dcc0-42b8-820c-001e185e81db.__CR0,4,300,300_PT0_SX300_V1___.png'




    # urllib.request.urlretrieve(Link4, "sample.png")
    # imzz = PIL.Image.open("sample.png")
    # imzz.show()

    # # Grayscale image
    # img = Image.open('sample.png').convert('L')
    # ret,img = cv2.threshold(np.array(img), 125, 255, cv2.THRESH_BINARY)


    # img = Image.fromarray(img.astype(np.uint8))
    # pytesseract.pytesseract.tesseract_cmd = r'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'
    # text = pytesseract.image_to_string(img)


