"""
NORM results.py

Josh Marshall 2010
This file contains the Results class.
"""
from connection import connection, cursor
from fields import ReferenceField
import types

ASCENDING = 'ASC';
DESCENDING = 'DESC';

class Results(object):
    
    def __init__(self, model):
        self.model = model
        self.fields = self.model.fields()
        self.values = []
        self.where_fields = {}
        self.order_fields = {}
        self.current_row = 0
        self.slice = slice(None, None, None)
    
    def where(self, limiter={}):
        """
        Takes a limiter dict with key=>value relationships,
        and adds them to the self.where_fields dict, normalizing
        values as necessary.
        
        TODO: Add JOINs.
        """
        from model import Model
        self.where = u''
        if issubclass(type(limiter), Model):
            limiter = get_model_limiter(limiter)
        self.where_fields = {}
        for column,value in limiter.iteritems():
            """
            The key is the attribute name, the column is either
            the ReferenceField or the 'table.column' string, and
            the value is either the ReferenceField or the formatted
            value from the appropriate model.
            """
            if type(column) is not ReferenceField:
                key = column
                column = '%s.%s' % (self.model.table(), column)
            else:
                key = column.ref_model.get_primary()
            if type(value) is not ReferenceField:
                # Formatting value appropriately...
                attr = object.__getattribute__(self.model, key)
                attr.value = value
                value = attr._value
            self.where_fields[column] = value
        return self
        
    def order(self, column, direction=ASCENDING):
        """
        Simply stores a columns order into the order
        field dict. Must be 'ASC' or 'DESC'.
        """
        assert direction in [ASCENDING, DESCENDING]
        self.order_fields[column] = direction;
        return self
        
    def reverse(self):
        """
        This reverses all order values, so that if there are no
        specific orders it will reverse by primary key, otherwise
        it reverses all the ASCENDING to DESCENDING and vice versa.
        """
        if len(self.order_fields) == 0:
            """ Only auto-reversing primary if nothing else specified """
            if not self.order_fields.has_key(self.model.get_primary()):
                """ Putting it in so it will be reversed. """
                self.order_fields[self.model.get_primary()] = ASCENDING
        for k,v in self.order_fields.iteritems():
            if v == ASCENDING:
                self.order_fields[k] = DESCENDING
            else:
                self.order_fields[k] = ASCENDING
        return self
        
    def delete(self):
        """
        This deletes all the entries that match the current conditions.
        """
        self.operation = u"DELETE FROM %s" % self.model.table()
        return self
        
    def update(self, set_values={}):
        """
        This updates all the entries that match the current conditions,
        using the dict passed in.
        """
        sets = []
        values = []
        obj = self.model()
        for f,v in set_values.iteritems():
            setattr(obj, f, v)
            attr = object.__getattribute__(obj, f)
            sets.append(u'%s = %s' % (f, attr.format))
            values.append(attr._value)
        set_sql = u'SET %s' % u', '.join(sets)
        self.operation = 'UPDATE %s %s' % (self.model.table(), set_sql)
        self.values = values + self.values
        return self
        
    def fetch_one(self):
        """
        The fetch_one method returns the first result of the
        __iter__() result using the limiter object. 
        """
        for result in self:
            return result
        return None
        
    def __call__(self):
        """
        This executes the behavior without the user needing to iterate.
        Should only be used with multi-result updates and deletes.
        """
        return self.__iter__()
        
    run = __call__
        
    def get_sql(self):
        """
        Parses the values and generates final SQL for execution.
        """
        tables = [self.model.table(),]
        where = u''
        order = u''
        limit = u''
        
        # WHERE instructions
        if len(self.where_fields) > 0:
            where_clauses = []
            for key, value in self.where_fields.iteritems():
                if type(key) is ReferenceField:
                    ref = key.ref_model
                    key = u'%s.%s' % (ref.table(), ref.get_primary())
                    if ref.table() not in tables:
                        tables.append(ref.table())
                if type(value) is ReferenceField:
                    ref = value.ref_model
                    value = u'%s.%s' % (ref.table(), ref.get_primary())
                    if ref.table() not in tables:
                        tables.append(ref.table())
                else:
                    # MySQLdb string substitution
                    self.values.append(value)
                    value = u'%s'
                where_clauses.append(u'%s = %s' % (key, value))
            where = u' WHERE %s' % ' AND '.join(where_clauses)
        
        # ORDER instructions (only will be used for SELECT)
        if len(self.order_fields):
            order_clauses = []
            for k,v in self.order_fields.iteritems():
                order_clauses.append(u'%s %s' % (k, v))
            order = u' ORDER BY %s' % ' AND '.join(order_clauses)
            
        # LIMIT instructions
        if self.slice.stop != None and self.slice.stop > 0:
            limit = u' LIMIT %d' % self.slice.stop
            
        # SELECT statement if operation not set by delete(), insert(), etc.
        if not hasattr(self, 'operation'):
            fields = [u'%s.%s' % (self.model.table(), f) for f in self.fields]
            self.operation = u"SELECT %s FROM %s" % \
                (u', '.join(fields), u', '.join(tables))
        else:
            # No order for DELETE, UPDATE, etc.
            order = u''
            
        return u'%s%s%s%s;' % (self.operation, where, order, limit)
        
    def __iter__(self):
        """
        Gets the SQL and makes the call, then stores
        the result on the class for the __next__() call(s).
        """
        if not connection.connected:
            raise Exception('Not connected to the database.')
        if not hasattr(self, 'cursor'):
            sql = self.get_sql()
            self.cursor = connection.execute(sql, tuple(self.values))
        return self
        
    def next(self):
        """
        Grabs the next result (if there is one) and returns
        a matching object for it.
        
        TODO: Implement the "step" part of the slice.
        """
        
        if not self.operation.startswith('SELECT'):
            raise StopIteration
        
        if self.current_row == 0 and self.slice.start != None:
            index = 0
            if self.slice.start < 0:
                index = self.cursor.rowcount + self.slice.start
            elif self.slice.start > 0:
                index = self.slice.start
            if index >= self.cursor.rowcount:
                raise IndexError("Start value beyond number of rows.")
            self.cursor.scroll(index, 'absolute')
            self.current_row = index
        
        if self.slice.stop != None:
            stop = self.slice.stop
            if self.slice.stop < 0:
                stop = self.cursor.rowcount + self.slice.stop
            if self.current_row >= stop:
                raise StopIteration  
        
        result = self.cursor.fetchone()
        if result == None:
            raise StopIteration
        obj = self.model()
        for i in range(len(self.fields)):
            object.__getattribute__(obj, self.fields[i])._value = result[i]
        object.__setattr__(obj, '_retrieved', True)
        self.current_row += 1
        return obj
        
    def __getitem__(self, key):
        """
        Chooses a slice or row from the result
        """
        if type(key) not in [types.SliceType, types.IntType]:
            raise TypeError
        if type(key) is types.IntType:
            self.slice = slice(key, key+1)
            for i in self:
                return i
        else:
            self.slice = key
            return self
            
    def limit(self, stop):
        """
        Just a quick wrapper around __getitem__
        """
        return self.__getitem__(slice(0, stop))
        
    def __len__(self):
        """
        Returns the number of rows from selection. Only works
        if __iter__ has already been called -- may need to 
        patch this.
        """
        if not hasattr(self, 'cursor'):
            results = self.__iter__()
        return self.cursor.rowcount
            
    def get_model_limiter(self, instance):
        """
        Returns a {primary_col:primary_key} for
        an instance of a different model. Usually
        used with ReferenceFields.
        """
        cls = instance.__class__
        primary = cls.get_primary()
        return { primary: getattr(instance, primary) }
