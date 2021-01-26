# SQL-Engine

  - ***Mini-SQL-Engine*** engine in ***Python*** which will run a subset of SQL Queries using command line interface.
  - ## **Dataset**
    - all elements in the fields would be ***integers*** only
    - file is named ***table1.csv***, the database table name would be ***table1***
    - ***metadata.txt*** file will be given for schema of corresponding database tables
      ```
      <begin_table>
      <table_name>
      <attribute1>
      ....
      <attributeN>
      <end_table>
      ```
  - ## **Queries**
    - **Select** all records: ``` SELECT * FROM table_name; ```
    - **Aggregate** functions: ***sum, average, min, max*** ``` SELECT MAX(col) FROM table_name; ```
    - **Project** coloumns: ``` SELECT col FROM table_name; ```
    - Project with **distinct**: ``` SELECT DISTINCT col FROM table_name; ```
    - Select from **one or more tables**: ``` SELECT col1, col2 FROM table1, table2 WHERE col1=10 AND col2=20; ```
    - Projection from one or more from two tables with **one join condition**: 
    ```
    a. SELECT * FROM table1, table2 WHERE table1.col1=table2.col2;
    b. SELECT col1, col2 FROM table1, table2 WHERE table1.col1=table2.col2; 
    ```
    - Queries with group by and/or order by clause is handled
    - Basic Error Handling is done
  - ## **Execute**
    - run Mini-SQL-Engine from **python file** ``` python sql.py "<query>" ```
    - run Mini-SQL-Engine from **bash script** ``` bash 2020201061.sh ```
