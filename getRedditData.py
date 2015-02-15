import praw
import sqlite3
from submission import Submission
from comment import Comment

#helper functions 
def read_replies(comment):
    """A helper to read the tree of replies for a comment"""
    reply_list = []
    for reply in comment.replies:
        #exclude deleted comments and all children
        if reply.author is not None:
            print type(reply)
            print reply.id
            print reply.author
            reply_list.append(Comment.read_raw_com(reply))
            reply_list.extend(read_replies(reply))
    return reply_list

# define some constants
NUM_TOP_SUBMISSIONS = 100
NUM_TOP_COMMENTS = 100
MAX_COMMENT_LEVEL = 3 #how far down the comment tree should comments be read
SUBREDDIT = "IAmA"
USER_AGENT = ("Test script 0.1 by /u/profound3")
DB_NAME = "raw_subreddit.db"

r = praw.Reddit(user_agent = USER_AGENT)

submissions = r.get_subreddit(SUBREDDIT).get_hot(limit = NUM_TOP_SUBMISSIONS)

#note use dir(object) to see an array the name of its members
submission_list = []
comment_list = []
for next_sub in submissions:
    submission_list.append(Submission.read_raw_sub(next_sub))
    #get the desired number of comments
    next_sub.replace_more_comments(limit = NUM_TOP_COMMENTS)
    for comment in next_sub.comments:
        #exclude deleted comments and all children
        if comment.author is not None:
            comment_list.append(Comment.read_raw_com(comment))        
            comment_list.extend(read_replies(comment))

# write data to database
# create submission and comment table if they do not exist in db
Submission.create_table(DB_NAME)
Comment.create_table(DB_NAME)

# 
Comment.write_db_list(comment_list, DB_NAME)
Submission.write_db_list(submission_list, DB_NAME)
