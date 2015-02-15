import sqlite3 as lite

class Comment:
    """A class for scrapped reddit comments"""
    def __init__(self, comment_id, author, created_utc, score, num_replies, 
                 submission_id, parent_id, text):
        self.comment_id = comment_id
        self.author = author
        self.created_utc = created_utc
        self.score = score
        self.num_replies = num_replies
        self.submission_id = submission_id
        self.parent_id = parent_id
        self.text = text
        
    @classmethod
    def read_raw_com(cls, raw_comment):
        """Creates a Comment from a praw.objects.Comment"""
        return cls(raw_comment.id.encode('ascii', 'replace'),
                   raw_comment.author.name.encode('ascii', 'replace'),
                   raw_comment.created_utc,
                   raw_comment.score,
                   len(raw_comment.replies),
                   raw_comment.submission.id,
                   raw_comment.parent_id.rsplit('_',1)[1],
                   raw_comment.body.encode('ascii', 'replace'))

    @classmethod
    def read_db(cls, db_name, row):
        """Creates a Comment by reading from a database"""
        con = lite.connect(db_name)
        
        with con:
            
            cur = con.cursor()
            cur.execute("SELECT * FROM Comments")
            com_tuple = cur.fetchone()

        return cls(*com_tuple)

    @classmethod
    def create_table(cls, db_name):
        """Creates a Comments table in a database if it does not exist"""
        con = lite.connect(db_name)

        with con:

            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Comments(comment_id TEXT, "
                        "author TEXT, created_utc INTEGER, score INTEGER, "
                        "num_comments INTEGER, submission_id TEXT, "
                        "parent_id TEXT, text TEXT)")

    @classmethod
    def write_db_list(cls, comment_list, db_name):
        """Writes a Comment list to a database efficiently"""
        tuple_list = []
        for comment in comment_list:
            tuple_list.append(comment.get_tuple())
        
        con = lite.connect(db_name)

        with con:

            cur = con.cursor()
            cur.executemany("INSERT INTO Comments VALUES (?,?,?,?,?,?,?,?)",
                        tuple_list)

    def write_db(self, db_name):
        """Writes the Comment to a database"""
        con = lite.connect(db_name)

        with con:

            cur = con.cursor()
            cur.execute("INSERT INTO Comments VALUES (?,?,?,?,?,?,?,?)",
                        (self.comment_id, self.author, self.created_utc, 
                         self.score, self.num_replies, self.submission_id,
                         self.parent_id, self.text))

    def get_tuple(self):
        """Returns a tuple containing the comment data"""
        return((self.comment_id, self.author, self.created_utc, self.score,
                self.num_replies, self.submission_id, self.parent_id, 
                self.text))
