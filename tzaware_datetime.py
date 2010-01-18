#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
"""sqlalchemy timezone-aware datetime support

Usage:
  Add the following columns to the table definition:
    Column('utcdate', DateTime),
    Column('tzname', Unicode),
    Column('tzoffset', Integer))

  In the mapper, add the following key:
  'tzawaredate': composite(TZAwareDateTime, 
                            thetable.c.utcdate, 
                            thetable.c.tzname,
                            thetable.c.tzoffset)
                            
  The columns can be named anything, but they must exist with those types and be reference in that order.
"""
__version_info__ = ('0', '5', '0-alpha')
__version__ = '.'.join(__version_info__)
__author__ = 'Andrew Ittner <projects@rhymingpanda.com>'
# stdlib
from datetime import datetime, timedelta

# sqlalchemy
from sqlalchemy import Column, DateTime, Unicode, Integer
from sqlalchemy.orm import composite

# dateutil <http://labix.org/python-dateutil>
from dateutil import tz

# module-level data
TZAwareDateTimeColumns = (Column('utcdate', DateTime),
                          Column('tzname', Unicode),
                          Column('tzoffset', Integer))
TZAwareDateTimeColumnNames = ('utcdate', 'tzname', 'tzoffset')

class TZAwareDateTime(object):
    """A composite sqlalchemy column that round-trips timezone-aware datetime objects"""
    def __init__(self, utcdt=None, tzname=None, offsetseconds=None, realdate=None):
        """utcdt: UTC datetime
        tzname: human-readable timezone name
        offsetseconds: seconds between local date and UTC
        realdate: actual date in target timezone
        """
        if (realdate is None):
            self.utcdt = utcdt
            self.tzname = tzname
            self.offsetseconds = offsetseconds
        else:
            self._set_realdate(realdate)
            
    def __repr__(self):
        if self.tzname:
            return "<TZDateTime (%s, tzname=%s, offset=%s)>" % (self.realdate, 
                                                                self.tzname,
                                                                self.offsetseconds)
        else:
            return "<TZDateTime (%s, offset=%s)>" % (self.realdate, 
                                                     self.offsetseconds)
        
    def __composite_values__(self):
        return [self.utcdt, self.tzname, self.offsetseconds]

    def __set_composite_values__(self, utcdt, tzname, offsetseconds):
        self.utcdt = utcdt
        self.tzname = tzname
        self.offsetseconds = offsetseconds
        
    def __eq__(self, other):
        return other.utcdt == self.utcdt
    
    def __ne__(self, other):
        return not self.__eq__(other)

    def _calc_offset_seconds(self, tdelta):
        """calculate timedelta seconds based on day value"""
        assert isinstance(tdelta, timedelta)
        return (-tdelta.days * 86400) - tdelta.seconds
    
    def _get_realdate(self):
        """reconstruct timezone-aware date from 3 columns"""
        tz_reconstitute = None
        # use offset from UTC (timezone name not guaranteed for roundtrip)
        if self.offsetseconds is None:
            # return date as UTC
            if self.utcdt is None:
                return None
            return self.utcdt.replace(tzinfo=tz.tzutc())
        else:
            tz_reconstitute = tz.tzoffset(name=None, offset=self.offsetseconds)
            
        # get naive date in UTC timezone, convert to non-naive, offset to given timezone
        return self.utcdt.replace(tzinfo=tz.tzutc()).astimezone(tz_reconstitute)
    
    def _set_realdate(self, newdate):
        """use a single datetime with a timezone to set class values"""
        # convert to utc
        self.utcdt = newdate.astimezone(tz.tzutc())

        # get timezone name
        newtzname = newdate.tzname()
        if newtzname is not None:
            self.tzname = unicode(newtzname)
        else:
            self.tzname = None

        # set offset
        self.offsetseconds = self._calc_offset_seconds(newdate.utcoffset())
    
    realdate = property(_get_realdate, _set_realdate)

class helper(object):
    """functions to insert TZAwareDateTime into database objects"""

    @staticmethod
    def append_columns(newtable, columnname):
        """given a sqlalchemy Table, add the TZAwareDatetime Column objects to it
        Modifies newtable in place"""
        # set prefixes for these columns
        for c in TZAwareDateTimeColumns:
            # NOTE: a copy must be made first, then attributes set;
            #   setting name & key args in column.copy() does not work
            newcolumn = c.copy()
            #assert isinstance(newcolumn, Column)
            newcolumn.name = '%s_%s' % (columnname, c.name)
            newcolumn.key = '%s_%s' % (columnname, c.key)
            newtable.append_column(newcolumn)
            
    @staticmethod
    def get_mapper_definition(newtable, columnname):
        """Given a Table object, return the Mapper definition for a TZAwareDateTime column"""
        # cycle through columns, find the utcdate, tzname, tzoffset columns
        column_utcdate, column_tzname, column_tzoffset = None, None, None
        new_column_names = {'utcdate': '%s_%s' % (columnname, TZAwareDateTimeColumnNames[0]),
                            'tzname': '%s_%s' % (columnname, TZAwareDateTimeColumnNames[1]),
                            'tzoffset': '%s_%s' % (columnname, TZAwareDateTimeColumnNames[2])
                            }

        for c in newtable.c:
            assert isinstance(c, Column)
            if c.key == new_column_names['utcdate']:
                column_utcdate = c
            elif c.key == new_column_names['tzname']:
                column_tzname = c
            elif c.key == new_column_names['tzoffset']:
                column_tzoffset = c
            if column_utcdate != column_tzname != column_tzoffset != None:
                break

        return composite(TZAwareDateTime,
                         column_utcdate,
                         column_tzname,
                         column_tzoffset)

