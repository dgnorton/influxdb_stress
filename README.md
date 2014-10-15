influxdb_stress
===============

Command line app to stress test influxdb

Overview
========

Creates zero or more database writers and/or readers to stress test influxdb.
By default, it creates 10 of each.

Install
=======

Run `go get github.com/dgnorton/influxdb_stress` to install.

Usage
=====

Run `influxdb-stress -h` to show a list of command line options.

You can change the queries that the reader(s) run by editing the array of query templates and rebuilding influxdb_stress.
