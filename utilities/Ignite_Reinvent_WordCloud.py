# Databricks notebook source
!pip install wordcloud nltk youtube-transcript-api

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Get Transcripts from Youtube

# COMMAND ----------

# Download the youtube video transcript
from youtube_transcript_api import YouTubeTranscriptApi

# Replace 'video_id' with the ID of the YouTube video.
#video_id = 'FZhbJZEgKQ4' #MS-2023
video_id = 'pdSfgRYy8Ao' #MS-2022
#video_id = '8clH7cbnIQw' #AWS-2023

# Fetching the transcript.
transcript_list = YouTubeTranscriptApi.get_transcript(video_id)

# Combine all parts of the transcript into a single string
transcript_text = ' '.join([transcript['text'] for transcript in transcript_list])

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a Word Cloud of Common Words

# COMMAND ----------

# Remove STOP WORDS like "at", "the" and "and" - then create a word cloud by frequency of key terms in the transcript
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Download dics of words
nltk.download('stopwords')
nltk.download('punkt')

# Tokenize the text
words = word_tokenize(transcript_text)

# Filter out stop words and junk words
stop_words = set(stopwords.words('english'))
more_stop_words = ['use', 'well', 'azure', 'new', 'one', 'know', 'need', 'microsoft', 'aws', 'amazon', 'also', 'like']
all_stop_words = stop_words.union(more_stop_words)
keywords = [word.lower() for word in words if word.isalpha() and word.lower() not in all_stop_words]

# Calculate freq. dist.
freq_dist = nltk.FreqDist(keywords)

# Get the top N most common, after remove basic words
common_words = freq_dist.most_common(10)

# Create a string
word_freq = {word: freq for word, freq in common_words}

# WordCloud (WC)
wordcloud = WordCloud(width = 800, height = 800, 
                      background_color ='white', 
                      min_font_size = 10)

# Generage WC
wordcloud.generate_from_frequencies(word_freq)

# Plot and show                
plt.figure(figsize = (8, 8), facecolor = None) 
plt.imshow(wordcloud)
plt.axis("off") 
plt.tight_layout(pad = 0)
plt.show()
