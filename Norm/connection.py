"""
NORM connection.py

Josh Marshall 2010
This file contains the Connection class.
"""

import re
import MySQLdb
import logging

"""
This ensures that the library doesn't throw out a message if
no logging is specified.
"""

class NullHandler(logging.Handler):
    def emit(self, record):
        pass
        
norm_logger = logging.getLogger('Norm')
norm_logger.addHandler(NullHandler())

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
    _cursor = None
    
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
        self.logger = logging.getLogger('Norm')
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
        cursor = self.cursor
        cursor.execute(command, values)
        return cursor

    @property
    def cursor(self):
        if self._cursor:
            self._cursor.close()
        self._cursor = self.connection.cursor()
        return self._cursor
        
    def close(self):
        if self.connection:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            self.connection.close()
            self.connection = None
            
    def __del__(self):
        self.close()
        
        
""" The connection singleton. """        
connection = Connection()

""" The connect function """
def connect(*args, **kwargs): 
    return connection.connect(*args, **kwargs)

""" The cursor function """
def cursor():
    return connection.cursor
