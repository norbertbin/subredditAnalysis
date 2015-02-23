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
DB_NAME = "data/raw_subreddit.db"
PROC_DB_NAME = "data/proc_subreddit.db"
DTM_FILE = "data/dtm_subreddit.mat"
COM_TEXT_INDEX = 7
SUB_TEXT_INDEX = 6
SUB_TITLE_INDEX = 1
WORD_COUNT_CUTOFF = 10
STOPWORDS = stopwords.words('english') + ['ive', 'k', 'th', 'm', 'im', 'also']
###


### helper functions
def strip_tuple(tuple_list, tuple_index = 0):
    """Given a list of tuples, creates a list of elements at tuple_index"""
    elem_list = []
    for i in range(0, len(tuple_list)):
        elem_list.append(tuple_list[i][tuple_index])
    return elem_list


def xstr(s):
    """If None convert to empty string"""
    if s is None:
        return u''
    return s

def clean_text(text_list):
    """Removes capitol letters and punctuation from list of text."""
    remove_char = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~$1234567890'
    translate_table = dict((ord(char), None) for char in remove_char)
    translate_table[ord(u'\n')] = ord(' ')
    for i in range(0, len(text_list)):
        text_list[i] = (xstr(text_list[i])).lower().translate(translate_table)
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

def gen_dtm(text_data, vocab):
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

def gen_new_table(db_old, db_new, table, col_index, new_col_list, ord_users, ord_subs):
    """Create a new table with new data in col_index"""
    con = lite.connect(db_old)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM " + table)
        tuple_list = cur.fetchall()
    for i in range(0, len(new_col_list)):
        tuple_list[i] = replace_tuple(tuple_list[i], new_col_list[i], col_index)
    #anonymize username and submission id
    if(table == "Comments"):
        anon_users = anonymize(strip_tuple(tuple_list, 1), ord_users)
        anon_subs = anonymize(strip_tuple(tuple_list, 5), ord_subs)
        for i in range(0, len(new_col_list)):
            tuple_list[i] = replace_tuple(tuple_list[i], anon_users[i], 1)
            tuple_list[i] = replace_tuple(tuple_list[i], anon_subs[i], 5)
    elif(table == "Submissions"):
        for i in range(0, len(new_col_list)):
            tuple_list[i] = replace_tuple(tuple_list[i], i, 0)
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

def create_vocab_table(db, vocab):
    """Creates a table with vocab in db"""
    con = lite.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS Vocab(vocab TEXT)")
    for i in range(0, len(vocab)):
        vocab[i] = (vocab[i],)
    with con:
        cur = con.cursor()
        cur.executemany("INSERT INTO Vocab VALUES (?)", vocab)
    
def get_user_and_text(db):
    """Returns tuple with user name and comment text"""
    con = lite.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("SELECT author, GROUP_CONCAT(text, ' ') FROM Comments GROUP BY author")
        user_text_list = cur.fetchall()   
    return user_text_list    

def create_user_text_table(db, user_list, text_list):
    """Creates a table with user name and their text"""
    con = lite.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS User(user TEXT, text TEXT)")
    ut_list = []
    for i in range(0, len(user_list)):
        ut_list.append((i, text_list[i])) #use index for instead of username
    with con:
        cur = con.cursor()
        cur.executemany("INSERT INTO User VALUES (?,?)", ut_list)

def anonymize(labels, unique_ordered_labels):
    """Renames labels using index of unique_ordered_labels"""
    index_dict = dict((val, idx) for idx, val in enumerate(unique_ordered_labels))
    return [index_dict[x] for x in labels]

def get_sub_list(db):
    """a"""
    con = lite.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("SELECT sub_id FROM Submissions")
        sub_list = cur.fetchall()   
    return strip_tuple(sub_list)

###


### process and save data
# load data from database and do basic processing
sub_text_list = get_text_data(DB_NAME, "Submissions", "text")
com_text_list = get_text_data(DB_NAME, "Comments", "text")

user_and_text_list = get_user_and_text(DB_NAME)
user_list = strip_tuple(user_and_text_list, 0)
user_text_list = clean_text(strip_tuple(user_and_text_list, 1))

sub_list = get_sub_list(DB_NAME)

# get joint vocabulary for submissions and comments excluding low counts
vocab = gen_vocab(sub_text_list + com_text_list, WORD_COUNT_CUTOFF, 
                  STOPWORDS)

# generate document term matrices
sub_dtm = gen_dtm(sub_text_list, vocab)
user_dtm = gen_dtm(user_text_list, vocab)

# filter unused words from text lists
sub_text_list = remove_unused_words(sub_text_list, vocab)
com_text_list = remove_unused_words(com_text_list, vocab)
user_text_list = remove_unused_words(user_text_list, vocab)

#save document term matrices 
io.savemat(DTM_FILE, dict(sub_dtm = sub_dtm,
                          user_dtm = user_dtm))

# create submission and comment table if they do not exist in db
Submission.create_table(PROC_DB_NAME)
Comment.create_table(PROC_DB_NAME)

#load processed data to a database for use in R
create_vocab_table(PROC_DB_NAME, vocab)
create_user_text_table(PROC_DB_NAME, user_list, user_text_list)

gen_new_table(DB_NAME, PROC_DB_NAME, "Submissions", SUB_TEXT_INDEX, sub_text_list, user_list, sub_list)
gen_new_table(DB_NAME, PROC_DB_NAME, "Comments", COM_TEXT_INDEX, com_text_list, user_list, sub_list)
###


