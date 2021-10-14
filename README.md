# Real-time-sentiment-analysis-pipeline-using-spark
## 1.Overview 
This Documentation is about end-to-end real time spark streaming sentiment analysis model Where data is sent from the twitter API then inserted to a ML model to analyze its polarity (positive or negative) after that it is saved in a file with parquet format. 

## 2. Building the Model 
Model building and data preprocessing was built using pyspark module.  
#Data: data used for training was about 1.6 Million rows from Kaggle (https://www.kaggle.com/kazanova/sentiment140) 
It contained 6 fields but the needed where only two which are tweets and target(0=negative, 4=positive) 
Data was then spllited to training and testing [70%, 30%] respectively  
Function [preprocess(data)] was used to preprocess the data to be ready for our model the steps are: 
• Removing useless characters like (#, $, RT, etc.) 
• Tokenizing tweets into tokens (list of separate words) 
• Remove stop words that doesn’t affect the meaning of the sentence like (a, the, is, are, etc.) 
• Hashing the words into numeric features to be understood by the model. 

Training models:  After making some search I found out that logestic regression is one of the best models for sentiment analysis and aside to it I also chose Naïve Bayes.   
 
Models Evaluation:  Naïve bayes scored 38% accuracy while Logestic regression scored 72% then I increased the model regParam from 0.1 to 0.3 and the accuracy increased to 74%.  
 
Then the  logestic regression model was saved to be implemented in to the pipeline. 

## 3. Twitter API Connection: 
 
Tweepy, OAuthHandler modules was used for this part to connect to twitter API  
TweetListner : a StreamListener instance was created to connect to the API and send one tweet at a time, it contains three functions : 
• __init__ method initializes the socket of the Twitter API. 
• on_data is the function that reads the incoming tweet JSON file and extract  the tweet text only. 
• on_error which makes sure that the stream is working  
 
function sendData authenticate the API connection then stream the tweet (TweetListener object) with the specific keyword and language. 
 
Finally in the main function in Twitter connection script a TCP socket with local IP address and port is created. 

## 4.Building the spark streaming pipeline: 
A spark session is created, after that tweet data is retrieved from the socket then our model is loaded , preprocessing function same as the one in the model building phase is applied on the data and then text_classification function is applied where it receives the processed tweets and insert it to the model and return a spark dataframe with the tweet and its prediction. Then the data is saved in a parquet file every 60 second. 
