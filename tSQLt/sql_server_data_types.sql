create table #tmp (t varchar(50))

truncate table #tmp

insert into #tmp values
--Date and time
('date'),
('datetimeoffset'),
('datetime2'),
('smalldatetime'),
('datetime'),
('time'),
('timestamp'),
--Exact numerics
('bigint'),
('numeric'),
('bit'),
('smallint'),
('decimal'),
('smallmoney'),
('int'),
('tinyint'),
('money'),
--Approximate numerics
('float'),
('real'),
--Character strings
('char'),
('varchar'),
('text'),
--Unicode character strings
('nchar'),
('nvarchar'),
('ntext'),
--Binary strings
('binary'),
('varbinary'),
('image'),
--Other data types
('cursor'),
('rowversion'),
('hierarchyid'),
('uniqueidentifier'),
('sql_variant'),
('xml'),
('table'),
--Spatial Geometry Types
('geometry'),
--Spatial Geography Types
('geography')


select t,
 case 
  when t !='timestamp' and (t like '%time%' or t = 'date') then 'dt' 
  when t  like '%int%' then 'int' 
  else 'not supported' 
  end
from #tmp

/*
https://docs.microsoft.com/en-us/sql/t-sql/statements/create-partition-function-transact-sql?view=sql-server-ver15
not supported 
text, 
ntext, 
image, 
xml, 
timestamp, 
varchar(max), 
nvarchar(max), 
varbinary(max)
*/
