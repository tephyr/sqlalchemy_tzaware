sqlalchemy_tzaware
==================

A project to create a timezone-aware sqlalchemy composite column, to enable any database
to hold datetime information that sorts correctly (by UTC), and displays
correctly (with the original timezone).

This code was originally created against sqlalchemy_ 0.6, and has *not* been
verified to work against 0.7.

.. _sqlalchemy: http://www.sqlalchemy.org/
