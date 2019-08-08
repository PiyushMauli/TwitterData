#!/usr/bin/env python
# coding: utf-8

# In[114]:


import nltk
# tokenizing in various ways
from nltk.tokenize import word_tokenize
# stopwwords collection
from nltk.corpus import stopwords
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

import pickle
# lemmatize like stem
from nltk.stem import WordNetLemmatizer
 
# To Wrap up the sklearn classifiers to be used in NLP for classifying
from nltk.classify.scikitlearn import SklearnClassifier

from sklearn.metrics import *
from sklearn.naive_bayes import MultinomialNB

import numpy as np
import pandas as pd
import sys
import os


# In[2]:


a = sys.stdin.read()


# In[3]:


if a == "":
    sys.exit()


# In[170]:


class Preprocessor:

    def preprocessor(self,doc):
        lm = WordNetLemmatizer()
        preprop = lambda x: ' '.join([lm.lemmatize(word) for word in x.split() if word not in stopwords.words('english') and not(word.isalpha() or word.startswith('@') or (word in ['!','.',','])) ])
        return doc.apply(preprop)


# In[171]:


model_path = "/home/user/Twitter_Live/src/main/model.pkl"


# In[172]:


file = open(model_path,'rb')
pre_process =  pickle.load(file)
cv = pickle.load(file)
mnb = pickle.load(file)
file.close()


# In[173]:


st = "poor day,I'm doing bad"


# In[174]:


z = a.split("\n")


# In[175]:


df = pd.DataFrame()


# In[176]:


for each in z:
    df=df.append([each])


# In[177]:


df.head()


# In[178]:


new_df=pre_process.preprocessor(df[0])


# In[179]:


new_df.head()


# In[180]:


transfromed = cv.transform(new_df[0])


# In[181]:


transfromed


# In[182]:


pred = mnb.predict(transfromed)


# In[183]:


pred_str = []


# In[184]:


for i in range(0,len(pred)):
    if pred[i] == 0:
        pred_str.append("negetive")
    else:
        pred_str.append("positive")


# In[188]:


# for each in df.values:
#     print(each)


# In[186]:


df['pred'] = pred_str


# In[190]:


# df.head()


# In[161]:


for each in df.values:
    print(each)


# In[ ]:


#newcols = ["tweet",]


# In[ ]:


#df.name(columns=newcols, inplace=True)

