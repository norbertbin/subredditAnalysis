import time
import requests
import praw
import sqlite3 as lite
from submission import Submission
from comment import Comment


### define some constants
NUM_TOP_SUBMISSIONS = 1000
NUM_TOP_COMMENTS = 1000
MAX_COMMENT_LEVEL = 3 #how far down the comment tree should comments be read
SUBREDDIT = "IAmA"
USER_AGENT = ("Test script 0.1 by /u/profound3")
DB_NAME = "data/raw_subreddit.db"
MAX_ERROR_COUNT = 20 #max number of connection errors allowed before termination
ERROR_SLEEP_TIME = 5  #sec sleep after connection error
###


#### helper functions 
def read_replies(comment):
    """A helper to read the tree of replies for a comment"""
    reply_list = []
    for reply in comment.replies:
        #exclude deleted comments and all children
        if reply.author is not None:
            reply_list.append(Comment.read_raw_com(reply))
            reply_list.extend(read_replies(reply))
    return reply_list
###


### pull data from reddit
# initialize subreddit connection
r = praw.Reddit(user_agent = USER_AGENT)
submissions = r.get_subreddit(SUBREDDIT).get_hot(limit = NUM_TOP_SUBMISSIONS)

# loop through all submissions first then pull data and comments
# this minimizes the effect of list order changes on reddit
error_count = 0
raw_submission_list = []
while True:
    try:
        next_sub = next(submissions)
        raw_submission_list.append(next_sub)
    except StopIteration:
        break
    except (requests.ConnectionError, requests.HTTPError) as e:
        if(error_count < MAX_ERROR_COUNT):
            error_count += 1
            time.sleep(ERROR_SLEEP_TIME)
        else:
            raise NameError('MaxConnectionErrors')
    except Exception as e:
        print e.__doc__

submission_list = []
comment_list = []
i = 0
while i < len(raw_submission_list):
    try:
        next_sub = raw_submission_list[i]
        submission_list.append(Submission.read_raw_sub(next_sub))
        #get the desired number of comments
        next_sub.replace_more_comments(limit = NUM_TOP_COMMENTS)
        for comment in next_sub.comments:
            #exclude deleted comments and all children
            if comment.author is not None:
                comment_list.append(Comment.read_raw_com(comment))        
                comment_list.extend(read_replies(comment))
        i += 1
    except (requests.ConnectionError, requests.HTTPError) as e:
        if(error_count < MAX_ERROR_COUNT):
            error_count += 1
            time.sleep(ERROR_SLEEP_TIME)
        else:
            raise NameError('MaxConnectionErrors')
    except Exception as e:
        print e.__doc__
        i += 1 #try skiping this submission
###


### write to database
# create submission and comment table if they do not exist in db
Submission.create_table(DB_NAME)
Comment.create_table(DB_NAME)

# write comment and submission data to db
Comment.write_db_list(comment_list, DB_NAME)
Submission.write_db_list(submission_list, DB_NAME)
###
