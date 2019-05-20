## SQL Query Engine - CSE-562

All the work is done as the part of the semester long project for [CSE-4/562 Database Systems (Spring 2019)](https://odin.cse.buffalo.edu/teaching/cse-562/2019sp/index.html) taught by [Professor Oliver Kennedy](https://odin.cse.buffalo.edu/people/oliver_kennedy.html).

The goal of the project is to implement complete SQL interpreter using Java. The libraries allowed to use have been included. The description of the jar files can be found here-

**Libraries included -**
- JSQLParser-UB ([Jar](http://maven.mimirdb.info/info/mimirdb/jsqlparser/1.0.0/jsqlparser-1.0.0.jar) | [JavaDoc](http://doc.odin.cse.buffalo.edu/jsqlparser/) | [Source](https://github.com/UBOdin/jsqlparser))
- EvalLib ([JAr](http://maven.mimirdb.info/info/mimirdb/evallib/1.0/evallib-1.0.jar) | [Source](https://github.com/UBOdin/evallib))
- Apache Commons CSV ([Jar](http://commons.apache.org/proper/commons-csv/download_csv.cgi) | [Source](https://commons.apache.org/proper/commons-csv/archives/1.4/apidocs/index.html))

**Experimentation Data -** 

TPCH Datagenerator can be used to download the data. The code has been well tested on 1GB data for limited as well as non restricted memory queries. The intructions to download data generator and setting it up are available [here](https://mapr.com/support/s/article/How-to-do-TPC-H-data-generation?language=en_US).


*The project has been divided into 4 checkpoints -* 

### - Checkpoint-1

**Checkpoint 1**: Basic Queries
**Overview**: Submit a simple SPJUA query evaluator.
**Complete Description** - [The ODIn Lab - CSE-4/562; Checkpoint 1](https://odin.cse.buffalo.edu/teaching/cse-562/2019sp/checkpoint1.html)

In this project, you will implement a simple SQL query evaluator with support for Select, Project, Join, and Bag Union operations.  You will receive a set of data files, schema information, and be expected to evaluate multiple SELECT queries over those data files. Your code is expected to evaluate the SELECT statements on provided data, and produce output in a standardized form. Your code will be evaluated for both correctness and performance (in comparison to a naive evaluator based on iterators and nested-loop joins).

#### Implementation Details - 

- This checkpoint acted as the foundation for the entire project. 
- Here we decided to opt for volcano style evaluation of queries i.e. creating a RA Tree by parsing the Query. All operators of a RA Tree act as a Node in the tree created using recursion. 
- Now, we created a simple interface i.e. RAIteraor which has methods *hasNext*, *next*, and *reset* as their base units. These methods needs to be implemented for all nodes of the tree. 
- Now all the nodes of the tree can be instances of 
    - tableIterator
    - JoinIterator
    - MapIterator
    - UnionIterator
    - FilterIterator
- The RA Tree has been created using the IteratorBuilder. Then next and hasNext method has been called repeatedly until we have all the results. Each Iterator calls it's child's methods. Also tableIterator accesses the file using a Buffered File Iterator. 
- A schema object has been created for each iterator which handles the schema at that point. 
- The expressions have been evaluated using evallib wherever necessary, for example Join On expression, and filter where expression.
- A CommonLib was created as a helper class. This class provided efficient methods to use eval by wraping the objects in easy way and passed to the evalLib. The PrimitiveValueWrapper is cretaed before passing anything to the eval. The wrapper involves the tuple, schema object(ColumnDefinitions, TableName), and TableName(could also be alias).

### - Checkpoint-2

**Overview**: New SQL features, Limited Memory, Faster Performance
**Complete Description** - [The ODIn Lab - CSE-562; Project 2](https://odin.cse.buffalo.edu/teaching/cse-562/2019sp/checkpoint2.html)

This project follows the same outline as Checkpoint 1. Your code gets SQL queries and is expected to answer them. There are a few key differences:

- Queries may now include a ORDER BY clause.
- Queries may now include a LIMIT clause.
- Queries may now include aggregate operators, a GROUP BY clause, and/or a HAVING clause.
- For part of the workload, your program will be re-launched with heavy restrictions on available heap space (see Java's -XMx option). You will most likely have insufficient memory for any task that requires O(N)-memory.

**Sorting and Grouping Data** -

Sort is a blocking operator. Before it emits even one row, it needs to see the entire dataset. If you have enough memory to hold the entire input to be sorted, then you can just use Java's built-in Collections.sort method. However, for the memory-restricted part of the workflow, you will likely not have enough memory to keep everything available. In that case, a good option is to use the 2-pass sort algorithm that we discussed in class.

**Join Ordering** -

The order in which you join tables together is incredibly important, and can change the runtime of your query by multiple orders of magnitude.  Picking between different join orderings is incredibly important!  However, to do so, you will need statistics about the data, something that won't really be feasible until the next project.  Instead, here's a present for those of you paying attention.  The tables in each FROM clause are ordered so that you will get our recommended join order by building a left-deep plan going in-order of the relation list (something that many of you are doing already), and (for hybrid hash joins) using the left-hand-side relation to build your hash table.

**Query Rewriting** - 

In Project 1, you were encouraged to parse SQL into a relational algebra tree.  Project 2 is where that design choice begins to pay off.  We've discussed expression equivalences in relational algebra, and identified several that are always good (e.g., pushing down selection operators). The reference implementation uses some simple recursion to identify patterns of expressions that can be optimized and rewrite them.  For example, if I wanted to define a new HashJoin operator, I might go through and replace every qualifying Selection operator sitting on top of a CrossProduct operator with a HashJoin.


#### Implementation Details - 
- The task now involved two major parts, i.e. on disk processing, and aggregation queries. 
- For all the new operators seperate iterators were built - 
    - HavingIterator
    - LimitIterator
    - GroupByIterator
    - OrderByIterator
    - aggregateIterator
- To tackle the problem of on disk implementations we have created a new helper method called sort, and fileIterator along with it. The sort object can be initialized whenever we need to sort some data. The sort has Constructor overloading to handle some different situations. The sort handles all the on disk sorting on its own using file Iterator. It splits data into multiple files and then uses Priority Queues to return the data back using methods. 
- Two new Join algorithms have been implemented i.e. One pass Hash Join, and Sorted Merge Join, one mainly for on disk implementation. The left table has been hashed in the one pass hash join this makes sure that we use a single pass through the entire data. For sorted merge join the sort objects have been created for both the left child and right child. The merge operation has been done by defining a comparator based on the On expression present in the Join Iterator.
- The Orderby iterator and group by iterator also use the same sort operator. 
- For the in memory version collections.sort has been used instead. 
- Along with this an optimizer is defined for faster query processing using selection pushdown, the optimizer recursively calls it's child's optimize method. The optimizer has been explicitely defined only in the FilterIterator, except that everywhere else only the recursivity has been handled. 

### - Checkpoint-3 - Indexes

**Overview**: Add a pre-processing phase to your system.
**Complete Description** - [The ODIn Lab - Checkpoint3](https://odin.cse.buffalo.edu/teaching/cse-562/2019sp/checkpoint3.html)

Once again, we will be tightening performance constraints.  You will be expected to complete queries in seconds, rather than tens of seconds as before.  This time however, you will be given a few minutes alone with the data before we start timing you.

Concretely, you will be given a period of up to 5 minutes that we'll call the Load Phase.  During the load phase, you will have access to the data, as well as a database directory that will not be erased in between runs of your application.  Example uses for this time include building indexes or  gathering statistics about the data for use in cost-based estimation.

Additionally, CREATE TABLE statements will be annotated with PRIMARY KEY and INDEX attributes.  You may also hardcode index selections for the TPC-H benchmark based on your own experimentation.

#### Implementation Details - 
- This checkpoint involved two phases where in one we had to create indexes and persist all the data we would require if the system shuts down after some queries. 
- The data used is TPCH Data which is already sorted on primary key. So indexes have been created on all secondary keys. This has been done by sorting the data according to the secondary key. The position values in byte position has been saved in the original file for all indexes. All this processing is completely generalized and performed by IndexMaker.
- Then the IndexIterator has been defined. Once the tree has been parsed the optimizer changes the tableIterator to indexIterator if index is present on that particular column of the filter iterator. THis makes solving that particular query very fast by using an index. 
- The index gives the value of all positions which need to be read. All required tuples have been read using the skip method of the buffered reader. 
- The Tuple helper is also implemented which makes converting string to primitivevalue faster as we only need to create and pass the ColumnDefinitions to the Tuple class once.
- To handle persisting the data a GlobalIndex file is created which saves all the CREATE table statements and the names of all the columns and tables for which we have indexes. As soon as the code executes it looks for this particular file to check. 
 
### - Checkpoint-4 - INSERTS, DELETES, & UPDATES

**Overview**: Support lightweight updates.
**Complete Description** - [The ODIn Lab - Checkpoint4](https://odin.cse.buffalo.edu/teaching/cse-562/2019sp/checkpoint4.html)

**Short version**: Support queries of the following three forms:
```bash
INSERT INTO R (A, B, C, ...) VALUES (1, 2, 3, 4, ...);
DELETE FROM R WHERE A < 3, ...
UPDATE R SET A = A+1, B = 2 WHERE C = 3;
```

Specifically, your code should support:

**INSERT** -
You only need to support INSERT ... VALUES (...) style inserts. There will not be any queries of any other form (i.e., nothing like INSERT ... SELECT ... or INSERT OR REPLACE ...)

**DELETE** -
Any selection predicate valid in a previous checkpoint is fair game for DELETE.

**UPDATE** -
Any selection predicate valid in a previous checkpoint is fair game for DELETE. Update expressions may include non-primitive value expressions.
Updates do not need to be persisted across database reboots, but should be reflected in query results.

#### Implementation Details - 
- Labda-Lite architecture has been used as we don't have to persist changes. Also all the processing has been done in memory. Disk has not been used for any new insertions. 
- Inserts have been stored in the appropriate data structures and updates and deletes have been applied to these as soon as we recieve some new operation. This makes interleaving of operations very simple for that particular part. 
- For all the on disk dat we need to apply the deletes and updates in the interleaved manner as we recieved them. This has been done using filter Iterator for the deletes, and updateItertor has been defined to handle updates. The updates have been handled using case statement which is solved by the evalLib.

