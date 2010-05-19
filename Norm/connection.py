"""
NORM connection.py

Josh Marshall 2010
This file contains the Connection class.
"""

import re
import MySQLdb

class Connection(object):
    """ 
    The Connection class is a simple wrapper around the
    MySQLdb connection and cursor properties / methods.
    """    
    host = None
    user = None
    password = None
    port = None
    db = None
    connection = None
    
    def connect(self, db_uri, verbose=False):
        """
        Connects to a database with the given DB URI, and
        keeps the connection on the object.
        """
        self.get_from_uri(db_uri)
        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=self.db,
            use_unicode=True
        )
        self.verbose = verbose
        
    @property
    def connected(self):
        """
        Determines whether we're connected.
        TODO: Make this actually, you know, work.
        """
        if self.connection:
            return True
        return False
        
    def get_from_uri(self, db_uri):
        """
        Understands DB URI's like mysql://test:test@localhost/test , 
        although it assumes everything is MySQL right now. :)
        """
        sql_re = re.compile('^(?P<type>[\w]+):\/\/(((?P<user>[\w\-]+)?' + \
            '(:(?P<pass>[^:]+))?@)?(?P<host>[\w\-\.]+)(:(?P<port>[\d]+))' + \
            '?)?\/(?P<db>.+)')
        result = sql_re.match(db_uri)
        if result:
            self.host = result.group('host')
            self.password = result.group('pass')
            self.user = result.group('user')
            self.port = result.group('port')
            self.db = result.group('db')
            
    def execute(self, command, values=()):
        """
        Simply a wrapper around the MySQLdb execute method        
        """
        if self.verbose:
            print command % tuple(values)
        cursor = self.connection.cursor()
        cursor.execute(command, values)
        return cursor

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor
        
    def close(self):
        if self.connection:
            self.connection.cursor.close()
            self.connection.close()
        
""" The connection singleton. """        
connection = Connection()

""" The connect function """
connect = connection.connect

""" The cursor function """
def cursor():
    return connection.cursor
