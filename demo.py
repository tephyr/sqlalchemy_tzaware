#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# sqlalchemy
from sqlalchemy import MetaData, Table, Column, DateTime, Unicode, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, relation, composite, create_session

# timezone-aware composite column
from tzaware_datetime import TZAwareDateTime

# demonstration "parent" class
class InfoMatic(object):
    """Holds basic info, plus a composite TZAwareDateTime column"""
    def __init__(self, info, tzawaredate, expectedoffset):
        self.info = info
        self.tzawaredate = tzawaredate
        self.expectedoffset = expectedoffset
    def __repr__(self):
        return "<InfoMatic('%s', %s)" % (self.info, self.tzawaredate)

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
                      Column('utcdate', DateTime), # for tzawaredate
                      Column('tzname', Unicode), # for tzawaredate
                      Column('tzoffset', Integer)) # for tzawaredate
    
    # setup mappings
    mapper(InfoMatic, table_infomatic, properties={
        'info': table_infomatic.c.info,
        'expectedoffset': table_infomatic.c.expectedoffset,
        'tzawaredate': composite(TZDateTime, 
                                 table_infomatic.c.utcdate, 
                                 table_infomatic.c.tzname,
                                 table_infomatic.c.tzoffset)
    })

    # create all tables
    metadata.create_all(myengine)

def run_demo():
    """prep the database, create a session, run some example code"""
    global myengine

    # create session
    session = create_session(bind=myengine, autocommit=True, autoflush=True) #autoflush=True: key!
    
    # create & save info objects
    lots_of_dates = [InfoMatic(u"first date", TZAwareDateTime(realdate=datetime.now(tz.tzutc())), 0)]
    lots_of_dates.append(InfoMatic(u"null date", TZAwareDateTime(), None))
    lots_of_dates.append(InfoMatic(u"PST date", 
                                   TZAwareDateTime(realdate=datetime.now(tz.gettz("PST"))),
                                   -28800))
    lots_of_dates.append(InfoMatic(u"New Zealand date", 
                                   TZAwareDateTime(realdate=datetime.now(tz.gettz("Pacific/Auckland"),
                                                                         ))))
    session.add_all(lots_of_dates)
    
    # print all objects
    info_count = session.query(InfoMatic).count()
    print '\tAll info objects (%s)' % info_count
    for info in session.query(InfoMatic):
        assert isinstance(info, InfoMatic)
        if info.tzawaredate is not None:
            assert isinstance(info.tzawaredate, TZAwareDateTime)
        print info
        print info.info, info.tzawaredate.realdate, info.tzawaredate.utcdt
        
    session.close()

if __name__ == '__main__':
    run_demo()
    