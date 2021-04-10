### Base queries used for table profiling

# Common queries for both numeric and non-numeric columns
sqla = """
select count(*) as RowCount
, count(distinct {column}) as DistinctCount
, count_if({column} is NULL) NullCount 
from {table}
"""

sqlb = """
SELECT
  count_if(cnt=1) as UniqueCount
, count_if(cnt>1) as Duplicatecount 
from ( 
     SELECT {column}, count(*) as cnt FROM {table} 
     group by {column}
)
"""

# query for numeric columns
sql_num = """
select
  avg({column}) Average
, max({column}) Max
, min({column}) Min
, NULL as BlankCount
from {table} 
"""
# query for non-numeric columns
sql_char = """
with x as (
    select count_if(TRIM({column})= '') BlankCount
    from {table}
), y as (
    select 
      avg(length({column})) Average
    , max(length({column})) Max
    , min(length({column})) Min
    from {table} 
    where TRIM({column}) <> '' and {column} is not NULL
)
select Average, Max, Min, BlankCount
from y join x
"""
