# Berkeley DB Java Edition Playground

This is a playground for poking at bits of
[Berkeley DB Java Edition](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html). The
playground is built on top of [dropwizard](https://github.com/codahale/dropwizard) exposing several REST endpoints
backed by BDB as a data store.

## Data Resource

HTTP PUT JSON to /data/{object path} and it will be stored in BDB, keyed by the object path. HTTP GET
/data/{object path} to retrieve it. HTTP DELETE /data/{object path} to remove it. Simple, right?

## Changes Resource

HTTP GET /changes/{timestamp} to get all objects updated on or after the timestamp provided. This is supported
internally by a BDB
[SecondaryDatabase](http://download.oracle.com/docs/cd/E17277_02/html/java/com/sleepycat/je/SecondaryDatabase.html),
which is keyed by the modification time of the objects stored in the primary database.
