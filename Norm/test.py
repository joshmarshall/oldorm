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
    name = UnicodeField(length=300, index=10)
    state = ReferenceField(State)
    landlocked = BoolField()
    
class Person(Model):
    id = PrimaryField()
    name = UnicodeField(index=5, length=200)
    city = ReferenceField(City)
    created = CreatedField()
    updated = TimestampField()
    address = DictField()
    email = UnicodeField(length=100, unique=True)
    age = IntField(index=True, default=40)
    wage = FloatField(default=3.95)

state = State(name=u'Texas')
city = City(name=u'Austin')
city2 = City(name=u'Houston')

def run_test(func):
    start_message = 'Starting Task %s' % func.__name__
    print start_message
    print '='*len(start_message)
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
    city2.state = state
    city2.save()
    
def add_user():
    wilbur = Person(name=u'Wilbur', city=city)
    wilbur.age = 21
    wilbur.wage = 9.46
    wilbur.address = {
        'address':'200 W. Main', 
        'city':'Austin', 
        'state':'Texas'
    }
    wilbur.save()
    
def add_users():
    for i in range(1, 2001):
        user = Person(name=u'%d' % i , city=city)
        user.address = {
            'address':'200 W. Main', 
            'city':'Austin', 
            'state':'Texas'
        }
        user.save()
    print '%s user(s) added.' % (i+1)

def get_user():
    wilbur = Person.fetch_one({'name':u'Wilbur'})
    if not wilbur:
        print 'No result.'
        return
    for f in Person.fields():
        print '%s: %s' % (f, getattr(wilbur, f))
    
def get_users():
    people = Person.all().reverse()[5:20]
    i = 0
    for person in people:
        i += 1
    print '%s people found.' % i
    
def update_user():
    wilbur = Person.fetch_one({'name':u'Wilbur'})
    wilbur.name = u'Wilburt'
    address = wilbur.address
    address['city'] = 'Houston'
    wilbur.address = address
    wilbur.city = city2
    wilbur.save()
    
def update_users():
    print len(Person.where({'city':city}).update({'city':city2}).run())
    
def compare_users():
    wilbur = Person.fetch_one({'name':u'Wilburt'})
    other = Person.fetch_one({'name':u'%s' % 2})
    assert wilbur != other
    assert wilbur == wilbur
    
def delete_user():
    wilbur = Person.fetch_one({'name':u'Wilburt'})
    wilbur.delete()
    
def delete_users():
    print len(Person.where({'city':city}).delete().run())
    
def delete_tables():
    Person.drop_table()
    State.drop_table()
    City.drop_table()
  
def test(verbose=False):  
    connection.connect('mysql://test:test@localhost/test', verbose=verbose)
    delete_tables()
    for test in [
        create_tables, add_city, add_user,
        add_users, get_user, get_users, 
        update_user, update_users, compare_users, 
        delete_user, delete_users, delete_tables
    ]:
        run_test(test)
    print 'Finished running tests.'

if __name__ == '__main__':
    import sys
    verbose = False
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        verbose = True
    test(verbose)
