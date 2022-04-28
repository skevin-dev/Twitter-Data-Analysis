class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']
        
        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        self.df = self.df.drop_duplicates().drop_duplicates(subset='original_text')

        ---
        
        return df
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        ----
        self.df['created_at'] = pd.to_datetime(self.df['created_at'], errors='coerce')
        ----
        
        df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        
        ----
        # convert polarity column to numeric, coerce for setting Invalid parsing to nan
        df['polarity'] = pd.to_numeric(self.df['polarity'],errors="ignore")
        
        # same applies to other column
        df['subjectivity'] = pd.to_numeric(self.df['subjectivity'], errors="ignore")
        
        # putting square bracket while extracting column is the same putting a dot 
        df['favorite_count'] = pd.to_numeric( self.df.favorite_count, errors= "ignore")
        
        df['retweet_count'] = pd.to_numeric(self.df.retweet_count, errors="ignore")
        ----
        
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        # we use query function to query tweets whereby lang(language) = en(english)
        df = self.df.query("lang == 'en'")
        
        return df