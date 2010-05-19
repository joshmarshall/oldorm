"""
NORM results.py

Josh Marshall 2010
This file contains the Results class.
"""
from connection import connection, cursor
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
        
        TODO: Add JOIN and ReferenceField WHERE's.
        """
        from model import Model
        self.where = u''
        if issubclass(type(limiter), Model):
            limiter = get_model_limiter(limiter)
        for k,val in limiter.iteritems():
            if k.startswith('$'):
                # $lt, $gt, $inc, etc.
                del limiter[k]
                continue
            attr = object.__getattribute__(self.model, k)
            attr.value = limiter[k]
            limiter[k] = attr
        self.where_fields = {}
        for k, v in limiter.iteritems():
            self.where_fields[k] = v._value
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
            if not self.order_fields.has_key(self.model.primary()):
                """ Putting it in so it will be reversed. """
                self.order_fields[self.model.primary(), ASCENDING]
        for k,v in self.order_fields.iteritems():
            if v == ASCENDING:
                self.order_fields[k] = DESCENDING
            else:
                self.order_fields[k] = ASCENDING
        return self
        
        
    def fetch_one(self):
        """
        The fetch_one method returns the first result of the
        __iter__() result using the limiter object. 
        """
        for result in self:
            return result
        return None
        
    def get_sql(self):
        """
        Parses the values and generates final SQL for execution.
        """
        select = u"SELECT %s FROM %s" % \
            (u', '.join(self.fields), self.model.table())
        where = u''
        order = u''
        limit = u''
        
        if len(self.where_fields) > 0:
            where_clauses = []
            for k,v in self.where_fields.iteritems():
                where_clauses.append(u'%s = %s' % (k, u'%s'))
                self.values.append(v)
            where = u' WHERE %s' % ' AND '.join(where_clauses)
        
        if len(self.order_fields) > 0:
            order_clauses = []
            for k,v in self.order_fields.iteritems():
                order_clauses.append(u'%s %s' % (k, v))
            order = u' ORDER BY %s' % ' AND '.join(order_clauses)
        if self.slice.stop != None and self.slice.stop > 0:
            limit = u' LIMIT %d' % self.slice.stop
        return u'%s%s%s%s;' % (select, where, order, limit)
        
    def __iter__(self):
        """
        Gets the SQL and makes the call, then stores
        the result on the class for the __next__() call(s).
        """
        if not connection.connected:
            raise Exception('Not connected to the database.')
        sql = self.get_sql()
        self.cursor = connection.execute(sql, tuple(self.values))
        return self
        
    def next(self):
        """
        Grabs the next result (if there is one) and returns
        a matching object for it.
        
        TODO: Implement the "step" part of the slice.
        """
        
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
            if self.current_row > stop:
                raise StopIteration  
        
        result = self.cursor.fetchone()
        if result == None:
            raise StopIteration
        obj_dict = dict([
            (self.fields[i], result[i])
            for i in range(len(self.fields))
        ])
        obj = self.model(**obj_dict)
        self.current_row += 1
        return obj
        
    def __getitem__(self, key):
        """
        Chooses a slice or row from the result
        """
        if type(key) not in [types.SliceType, types.IntType]:
            raise TypeError
        if type(key) is types.IntType:
            self.slice = slice(key-1, key)
            for i in self:
                return i
        else:
            self.slice = key
            return self
        
    def __len__(self, key):
        """
        Returns the number of rows from selection. Only works
        if __iter__ has already been called -- may need to 
        patch this.
        """
        return self.cursor.rowcount
            
    def get_model_limiter(self, instance):
        """
        Returns a {primary_col:primary_key} for
        an instance of a different model. Usually
        used with ReferenceFields.
        """
        cls = instance.__class__
        primary = cls.primary()
        return { primary: getattr(instance, primary) }
