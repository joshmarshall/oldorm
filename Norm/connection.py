"""
NORM connection.py

Josh Marshall 2010
This file contains the Connection class.
"""

import re
import MySQLdb
import logging

class NullHandler(logging.Handler):
    """
    This ensures that the library doesn't throw out a message if
    no logging is specified.
    """
    def emit(self, record):
        pass
        
norm_logger = logging.getLogger('Norm')
norm_logger.addHandler(NullHandler())

class Connection(object):
    """ 
    The Connection class is a simple wrapper around the
    MySQLdb connection and cursor properties / methods.
    """    
    
    def __init__(self):
        self.host = None
        self.user = None
        self.password = None
        self.port = None
        self.db = None
        self.connection = None
        self._cursor = None
        self.verbose = False
        self.logger = logging.getLogger('Norm')
    
    def connect(self, db_uri, verbose=False):
        """
        Connects to a database with the given DB URI, and
        keeps the connection on the object.
        """
        if self.connection:
            return self
        self.get_from_uri(db_uri)
        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=self.db,
            use_unicode=True
        )
        self.verbose = verbose
        return self
        
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
        log_message = '%s@%s using %s: %s' % (
            self.user,
            self.host,
            self.db,
            command % tuple(values)
        )
        self.logger.debug(log_message)
        if self.verbose:
            print log_message
        new_cursor = self.cursor
        new_cursor.execute(command, values)
        return new_cursor

    @property
    def cursor(self):
        """
        Closes an open cursor (if applicable) and returns
        a new one from the current conneection.
        """
        if self._cursor:
            self._cursor.close()
        self._cursor = self.connection.cursor()
        return self._cursor
        
    def close(self):
        """
        Attemps to close the current connection and cursor.
        """
        if self.connection:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            self.connection.close()
            self.connection = None
            
    def __del__(self):
        self.close()
        object.__del__(self)
                       
# The connection singleton.
connection = Connection()  

def connect(*args, **kwargs):
    """ The connect function """
    return connection.connect(*args, **kwargs)

def cursor():
    """ The cursor function """
    return connection.cursor
