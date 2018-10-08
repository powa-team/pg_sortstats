pg_sortstats
============

** /!\ This extension is under development and not production ready.  Use at
your own risk. /!\ **

PostgreSQL extension to gather and cumulate various statistics about sorts, and
estimate how much work\_mem would be needed to have the sort done in memory.

Statistics are aggregated per queryid (query identifier as computed by
pg\_stat\_statements), userid, dbid and sort\_key (the textual representation
of the sort being performed).

pg\_stat\_statements is needed to provide the queryid field.

Installation
============

Compiling
--------

The module can be built using the standard PGXS infrastructure. For this to
work, the ``pg_config`` program must be available in your $PATH. Instruction to
install follows::

 git clone https://github.com/powa-team/pg_sortstats.git
 cd pg_sortstats
 make
 make install

NOTE: The "make install" part may require root privilege.

PostgreSQL setup
----------------

The extension is now available. But, as it requires some shared memory to hold
its counters, the module must be loaded at PostgreSQL startup. Thus, you must
add the module to ``shared_preload_libraries`` in your ``postgresql.conf``. You
need a server restart to take the change into account.  As this extension
depends on pg_stat_statements, it also need to be added to
``shared_preload_libraries``.

Add the following parameters into you ``postgresql.conf``::

 # postgresql.conf
 shared_preload_libraries = 'pg_stat_statements,pg_sortstats'

Once your PostgreSQL cluster is restarted, you can install the extension in
every database where you need to access the statistics::

 mydb=# CREATE EXTENSION pg_sortstats;

Usage
-----

The `pg_sortstats` view provides the following fields:

| fieldname       | description                                                                      |
|-----------------|----------------------------------------------------------------------------------|
| queryid         | pg_stat_statements' queryid                                                      |
| userid          | user identifier                                                                  |
| dbid            | database identifier                                                              |
| sort_key        | the textual sort expression                                                      |
| lines           | total number of lines the sort node had in input                                 |
| lines_to_sort   | total number of lines the sort node has to sort (different when there's a LIMIT) |
| work_mems       | total size of needed work_mem that was estimated to perform the sort in memory   |
| topn_sorts      | total number of sorts done using Top-N heapsort algorithm                        |
| quicksorts      | total number of sorts done using quicksort algorithm                             |
| external_sorts  | total number of sorts done using external sort algorithm                         |
| external_merges | total number of sorts done using external merge algorithm                        |
| nbtapes         | total number of tapes used for external merge sorts                              |
| space_disk      | total disk space used to perform the sort                                        |
| space_memory    | total memory space used to perform the sort                                      |
| non_parallels   | total number of sorts not done in parallel                                       |
| nb_workers      | total number of processes used to perform the sort                                 |

The `pg_sortstats(showtext)` can be used instead, passing **false** as
paremeter if you don't need the sort_key field.

The `pg_sortstats_reset()` function can be used to remove all stored
statistics.
