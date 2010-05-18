"""
NORM connection.py

Josh Marshall 2010
This file is just some simple CRUD tests. Run like so:

    python test.py
    
Or, to print out the SQL statements:

    python test.py -v

"""

from model import Model
from fields import *
import datetime
from connection import connection
import time
import random

class State(Model):
    id = PrimaryField()
    name = UnicodeField()
    
class City(Model):
    id = PrimaryField()
    name = UnicodeField()
    state = ReferenceField(State)
    
class Person(Model):
    id = PrimaryField()
    name = UnicodeField()
    city = ReferenceField(City, null=True)
    created = CreatedField()
    updated = TimestampField()

state = State(name=u'Texas')
city = City(name=u'Austin')

def run_test(func):
    print 'Starting Task %s' % func.__name__
    print '============='
    start = time.time()
    func()
    end = time.time()
    print 'Task finished in %s seconds' % (end-start)
    print
  
def create_tables():
    Person.create_table()
    State.create_table()
    City.create_table()
    
def add_city():
    state.save()
    city.state = state
    city.save()
    
def add_user():
    wilbur = Person(name=u'Wilbur', city=city)
    wilbur.save()
    
def add_users():
    for i in range(2000):
        user = Person(name=u'JOHNDOE', city=city)
        user.save()

def get_user():
    wilbur = Person.fetch_one({'name':u'Wilbur'})
    print 'Name: %s' % wilbur.name
    
def get_users():
    people = Person.fetch()
    i = 0
    for person in people:
        i += 1
    print '%s people found.' % i
    
def update_user():
    wilbur = Person.fetch_one({'name':u'Wilbur'})
    wilbur.name = u'Wilburt'
    wilbur.save()
    
def delete_tables():
    Person.drop_table()
    State.drop_table()
    City.drop_table()
  
def test(verbose=False):  
    connection.connect('mysql://test:test@localhost/test', verbose=verbose)
    for test in [
        create_tables, add_user, add_users,
        get_user, get_users, update_user,
        delete_tables
    ]:
        run_test(test)
    print 'Finished running tests.'

if __name__ == '__main__':
    import sys
    verbose = False
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        verbose = True
    test(verbose)
