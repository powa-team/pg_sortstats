pg_sortstats
============

PostgreSQL extension to gather statistics about sorts, and estimate how much
work\_mem would be needed to hame the sort done in memory.

The `pg_sortstats()` set-returning functions provides the following fields:

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
| nb_workers      | total number of workers used to perform the sort                                 |

