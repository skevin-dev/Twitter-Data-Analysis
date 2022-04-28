import json
import pandas as pd
from textblob import TextBlob

def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        #create an empty list 
        statuses_count = list()
        
        #iterate through tweets list 
        for x in self.tweets_list:
            #append in created list statuses count
            statuses_count.append(x['user']['statuses_count'])
                                  
    def find_full_text(self)->list:
        
        # create an empty list
        text = []
        for tweets in self.tweets_list:
            if "retweeted_status" in [keys for keys,values in tweets.items()] and "extended_tweet" in tweets['retweeted_status'].keys():
                text.append(tweets['retweeted_status']['extended_tweet']['full_text'])
            else:
                text.append("Empty")
        return text
                                  
         
    def find_sentiments(self, text)->list:
        polarity = []
        subjectivity = []
        for tweets in text:
            blob = TextBlob(tweets)
            sentiment = blob.sentiment
            polarity.append(sentiment.polarity)
            subjectivity.append(sentiment.subjectivity)
        return polarity, subjectivity

    def find_created_time(self)->list:
        created_at = []
        for x in self.tweets_list:
            created_at.append(x['created_at'])
       
        return created_at

    def find_source(self)->list:
        # I can also use list comprehensiom
        source = [x['source'] for x in self.tweets_list]

        return source

    def find_screen_name(self)->list:
        screen_name = []
        for x in self.tweets_list:
            screen_name.append(x['user']['screen_name']) 
        return screen_name
                                  
    def find_followers_count(self)->list:
        followers_count =[]
        for x in self.tweets_list:
            followers_count.append(x['user']['followers_count'])
        return followers_count

    def find_friends_count(self)->list:
        friends_count= []
        for x in self.tweets_list:
            friends_count.append(x['user']['friends_count'])
        return friends_count

    def is_sensitive(self)->list:
        try:
            is_sensitive = []
            for tweets in self.tweets_list:
                if 'possibly_sensitive' in [keys for keys,values in tweets.items()]:
                    is_sensitive.append(tweets['possibly_sensitive'])
                else:
                    is_sensitive.append(0)
                    
        except KeyError:
            is_sensitive = None

        return is_sensitive

    def find_favourite_count(self)->list:
        favorite_count = list()
        for tweets in self.tweets_list:
            if 'retweeted_status' in [keys for keys,values in tweets.items()]:
                favorite_count.append(tweets['retweeted_status']['favorite_count'])
            else:
                favorite_count.append(0)
        return favorite_count
        
    
    def find_retweet_count(self)->list:
        retweet_count = list()
        for tweets in self.tweets_list:
            if 'retweeted_status' in [keys for keys,values in tweets.items()]:
                retweet_count.append(tweets['retweeted_status']['retweet_count'])
            else:
                retweet_count.append(0)
        return retweet_count

    def find_hashtags(self)->list:
        hashtags = []

        for tweets in self.tweets_list:
            hashtags.append(",".join([x['text'] for x in tweets['entities']['hashtags']]))
        return hashtags

    def find_mentions(self)->list:
        mentions = []
        for tweets in self.tweets_list:
            mentions.append( ", ".join([x['screen_name'] for x in tweets['entities']['user_mentions']]))

        return mentions
                                  


    def find_location(self)->list:
        try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        
        return location

    def find_lang(self)->list:
        lang = []
        for x in self.tweets_list:
            lang.append(x['lang'])
        return lang
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = {"created_at":created_at,'source':source,'original_text':text,'polarity':polarity,'subjectivity':subjectivity,
                'lang':lang,'favorite_count':fav_count,'retweet_count':retweet_count,'original_author':screen_name, 
                'followers_count':follower_count,'friends_count':friends_count,'possibly_sensitive':sensitivity,
                'hashtags':hashtags,'user_mentions':mentions}
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("data/Economic_Twitter_Data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above

    