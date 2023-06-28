# Traditional Dw Etl

---

### Introduction

---

Simple ETL Tool for extract data from MySQL and PostgreSQL and storange on SQL Server. This example I have used Olist Kaggle Dataset and make a simple star schema model.

- Olist Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Star Schema Article: https://learn.microsoft.com/en-us/power-bi/guidance/star-schema

![Alt text](img/diagram.png)

The inspiration for this repository was native data integration tools like Talend and Pentaho.
With them we can easily do a lot of things without having to write a lot of code, so the idea was to try to do something similar writing in Python.

I tried to develop a solution with asynchronous loads of tables, for quick loads it works very well, I also put a functionality to include a certain processing date (cutoff date for collecting data from the source) so it is possible to limit and make the query faster for the load avoiding several problems.

In a simple Zoon-In on geral pileine whe get this simple process on image:

![image](https://github.com/xGabrielR/Traditional-Dw-Etl/assets/75986085/0bf0e1da-bc06-4513-b7a3-066a219a7aa6)


### Tools Used

---

Sql is the principal tool for this project, I have created all modeling and extraction based on sql query. 

Python Packages:
- `sqlalchemy` Package for SqlServer connection.
- `databases` Package for geral async databases connection.
- `aiomysql / asyncpg` Packages for async Mysql and Postgresql.
- `pandas` Package for data insert to Sql Server (Possible another backend insert like "Dask" with parallel insert).
- `memory-profiler` Package for memory monitoring.

For this solution I used docker containers for postgresql, mysql and sqlserver.
