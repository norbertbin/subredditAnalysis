import sklearn
import sqlite3 as lite
import string
from nltk.corpus import stopwords
from scipy import io
from collections import Counter
from itertools import dropwhile
from nltk.tokenize import word_tokenize
from submission import Submission
from comment import Comment


### define constants
DB_NAME = "data/test_raw_subreddit.db"
PROC_DB_NAME = "data/test_proc_subreddit.db"
DTM_FILE = "data/dtm_subreddit.mat"
COM_TEXT_INDEX = 7
SUB_TEXT_INDEX = 6
SUB_TITLE_INDEX = 1
WORD_COUNT_CUTOFF = 10
STOPWORDS = stopwords.words('english') + ['ive', 'k', 'th', 'm']
###


### helper functions
def strip_tuple(tuple_list, tuple_index = 0):
    """Given a list of tuples, creates a list of elements at tuple_index"""
    elem_list = []
    for i in range(0, len(tuple_list)):
        elem_list.append(tuple_list[i][tuple_index])
    return elem_list

def clean_text(text_list):
    """Removes capitol letters and punctuation from list of text."""
    remove_char = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~$1234567890'
    translate_table = dict((ord(char), None) for char in remove_char)
    translate_table[ord(u'\n')] = ord(' ')
    for i in range(0, len(text_list)):
        text_list[i] = (text_list[i]).lower().translate(translate_table)
    return text_list

def get_text_data(db, table, col):
    """Returns processed text from row in table"""
    con = lite.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("SELECT " + col + " FROM " + table)
        text_data = cur.fetchall() #list of tuples
        text_data = strip_tuple(text_data)
        text_data = clean_text(text_data)
        return(text_data)

def gen_DTM(text_data, vocab):
    """Creates document term count matrix"""
    vectorizer = sklearn.feature_extraction.text.CountVectorizer(
        vocabulary = vocab)
    return  vectorizer.fit_transform(text_data)

def gen_CRM(call_text, response_text):
    """Creates a call response matrix (count matching words)"""
    pass

def replace_tuple(tuple_obj, replace_obj, replace_index):
    """Create a new tuple with a new object at index"""
    if len(tuple_obj) - 1 <= replace_index:
        return tuple_obj[:replace_index] + (replace_obj,) 
    else:
        return tuple_obj[:replace_index] + (replace_obj,) + tuple_obj[replace_index+1:]

def gen_new_table(db_old, db_new, table, col_index, new_col_list):
    """Create a new table with new data in col_index"""
    con = lite.connect(db_old)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM " + table)
        tuple_list = cur.fetchall()
    for i in range(0, len(new_col_list)):
        tuple_list[i] = replace_tuple(tuple_list[i], new_col_list[i], col_index)
    num_bindings = len(tuple_list[0])
    bindings = ('?,' * num_bindings)[:-1]
    con = lite.connect(db_new)
    with con:
        cur = con.cursor()
        cur.executemany("INSERT INTO " + table + " VALUES" +  " ("+ bindings + ")", tuple_list)

def gen_vocab(text_list, cutoff, stopwords):
    """Generates a vocabulary in a dictionary for a list of text"""
    word_counts = Counter()
    for text in text_list:
        word_counts.update(word for word in text.split())
    # using dropwhile takes advantage of ordering
    for key, count in dropwhile(lambda key_count: key_count[1] >= cutoff, word_counts.most_common()):
        del word_counts[key]
    return list(set(word_counts.keys()) - set(stopwords))

def remove_unused_words(text_list, vocab):
    """Removes words not in vocab from a list of text"""
    vocabset = set(vocab)
    for i in range(0, len(text_list)):
        tokens = word_tokenize(text_list[i])
        tokens = [word for word in tokens if word in vocabset]
        text_list[i] = u' '.join(tokens)
    return text_list
###

### process and save data
# load data from database and do basic processing
sub_text_list = get_text_data(DB_NAME, "Submissions", "text")
com_text_list = get_text_data(DB_NAME, "Comments", "text")

# get joint vocabulary for submissions and comments excluding low counts
vocab = gen_vocab(sub_text_list + com_text_list, WORD_COUNT_CUTOFF, 
                  STOPWORDS)

# generate document term matrices
sub_dtm = gen_DTM(sub_text_list, vocab)
com_dtm = gen_DTM(com_text_list, vocab)

# filter unused words from text lists
sub_text_list = remove_unused_words(sub_text_list, vocab)
com_text_list = remove_unused_words(com_text_list, vocab)

#save document term matrices 
io.savemat(DTM_FILE, dict(sub_dtm = sub_dtm,
                          com_dtm = com_dtm))

# create submission and comment table if they do not exist in db
Submission.create_table(PROC_DB_NAME)
Comment.create_table(PROC_DB_NAME)

#load processed data to a database for use in R
gen_new_table(DB_NAME, PROC_DB_NAME, "Submissions", SUB_TEXT_INDEX, sub_text_list)
gen_new_table(DB_NAME, PROC_DB_NAME, "Comments", COM_TEXT_INDEX, com_text_list)
###

