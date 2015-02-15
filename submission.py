import sqlite3 as lite

class Submission:
    """A class for scrapped reddit submissions"""
    def __init__(self, sub_id, title, author, created_utc, score, 
                 num_comments, text):
        self.sub_id = sub_id
        self.title = title
        self.author = author
        self.created_utc = created_utc
        self.score = score
        self.num_comments = num_comments
        self.text = text

    @classmethod
    def read_raw_sub(cls, raw_submission):
        """Creates a Submission from a praw.objects.Submission"""
        return cls(raw_submission.id.encode('ascii', 'replace'),
                   raw_submission.title.encode('ascii', 'replace'),
                   raw_submission.author.name.encode('ascii', 'replace'),
                   raw_submission.created_utc,
                   raw_submission.score,
                   raw_submission.num_comments,
                   raw_submission.selftext.encode('ascii', 'replace'))

    @classmethod
    def read_db(cls, db_name, row):
        """Creates a Submission by reading from a database"""
        con = lite.connect(db_name)

        with con:

            cur = con.cursor()
            cur.execute("SELECT * FROM Submissions")
            sub_tuple = cur.fetchone()
            
        return cls(*sub_tuple)

    @classmethod
    def create_table(cls, db_name):
        """Creates a Submissions table in a database"""
        con = lite.connect(db_name)

        with con:
            
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Submissions(sub_id TEXT, "
                        "title TEXT, author TEXT, created_utc INTEGER, "
                        "score INTEGER, num_comments INTEGER, text TEXT)")

    @classmethod
    def write_db_list(cls, submission_list, db_name):
        """Writes a Submission list to a database efficiently"""
        tuple_list = []
        for submission in submission_list:
            tuple_list.append(submission.get_tuple())

        con = lite.connect(db_name)

        with con:

            cur = con.cursor()
            cur.executemany("INSERT INTO Submissions VALUES (?,?,?,?,?,?,?)", 
                        tuple_list)            

    def write_db(self, db_name):
        """Writes the Submission to a database""" 
        con = lite.connect(db_name)

        with con:

            cur = con.cursor()
            cur.execute("INSERT INTO Submissions VALUES (?,?,?,?,?,?,?)", 
                        (self.sub_id, self.title, self.author, 
                         self.created_utc, self.score, self.num_comments,
                         self.text)) 
            
    def get_tuple(self):
        """Return a tuple containing the submission data"""
        return((self.sub_id, self.title, self.author, self.created_utc,
                self.score, self.num_comments, self.text))
