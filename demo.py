#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
"""Demonstration of TZAwareDateTime composite column for sqlalchemy"""
__author__ = 'Andrew Ittner <aji@rhymingpanda.com>'
__copyright__ = "Public Domain (CC0) <http://creativecommons.org/publicdomain/zero/1.0/>"

# stdlib
from datetime import datetime

# sqlalchemy
from sqlalchemy import MetaData, Table, Column, DateTime, Unicode, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, relation, composite, create_session

# timezone-aware composite column
from tzaware_datetime import TZAwareDateTime

# 3rd-party: dateutil <http://labix.org/python-dateutil>
from dateutil import tz

# demonstration parent table
class InfoMatic(object):
    """sqlalchemy main demonstration table: contains basic info, plus a composite TZAwareDateTime column"""
    def __init__(self, info=None, tzawaredate=None, expectedoffset=None):
        self.info = info
        self.tzawaredate = tzawaredate
        self.expectedoffset = expectedoffset
    def __repr__(self):
        return "<InfoMatic('%s', %s, %s)" % (self.info, self.tzawaredate, self.expectedoffset)

def prep_database():
    global myengine

    # create engine
    myengine = create_engine('sqlite:///:memory:', echo=False)
    
    # setup table metadata
    metadata = MetaData()
    table_infomatic = Table('infomatic', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('info', Unicode(255)),
                      Column('expectedoffset', Integer),
                      Column('utcdate', DateTime), # for TZAwareDateTime
                      Column('tzname', Unicode), # for TZAwareDateTime
                      Column('tzoffset', Integer)) # for TZAwareDateTime
    
    # setup mappings
    mapper(InfoMatic, table_infomatic, properties={
        'info': table_infomatic.c.info,
        'expectedoffset': table_infomatic.c.expectedoffset,
        'tzawaredate': composite(TZAwareDateTime, 
                                 table_infomatic.c.utcdate, 
                                 table_infomatic.c.tzname,
                                 table_infomatic.c.tzoffset)
    })

    # create all tables
    metadata.create_all(myengine)

def run_demo():
    """prep the database, create a session, run some example code"""
    global myengine

    prep_database()
    
    # create session
    session = create_session(bind=myengine, autocommit=True, autoflush=True) #autoflush=True: key!
    
    # create & save info objects
    lots_of_dates = [InfoMatic(u"first date", TZAwareDateTime(realdate=datetime.now(tz.tzutc())), 0)]
    lots_of_dates.append(InfoMatic(u"null date", TZAwareDateTime(), None))
    lots_of_dates.append(InfoMatic(u"PST date", 
                                   TZAwareDateTime(realdate=datetime.now(tz.gettz("PST"))),
                                   28800))
    lots_of_dates.append(InfoMatic(u"New Zealand date", 
                                   TZAwareDateTime(realdate=datetime.now(tz.gettz("Pacific/Auckland"),
                                                                         ))))
    session.add_all(lots_of_dates)
    
    # print all objects
    info_count = session.query(InfoMatic).count()
    print '\tAll infomatic objects (%s)' % info_count
    for infomatic in session.query(InfoMatic):
        assert isinstance(infomatic, InfoMatic)
        if infomatic.tzawaredate is not None:
            assert isinstance(infomatic.tzawaredate, TZAwareDateTime)
        print infomatic
        print '\t', infomatic.info
        print '\ttzawaredate.realdate', infomatic.tzawaredate.realdate
        print '\ttzawaredate.utcdt', infomatic.tzawaredate.utcdt
        
    session.close()

if __name__ == '__main__':
    run_demo()
    