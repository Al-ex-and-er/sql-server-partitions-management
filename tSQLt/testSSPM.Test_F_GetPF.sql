CREATE PROCEDURE testSSPM.Test_F_GetPF
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetPF (int) AS RANGE RIGHT FOR VALUES (1, 2, 3)

CREATE PARTITION SCHEME PS_TestGetPF AS PARTITION PF_TestGetPF ALL TO ([PRIMARY])

CREATE TABLE TestGetPF(c1 int not null, c2 varchar(10)) on PS_TestGetPF(c1) with(data_compression = none) 

declare 
  @pf sysname = sspm.GetPF('TestGetPF'),
  @ps sysname = sspm.GetPS('TestGetPF')

exec tSQLt.AssertEquals @Expected = 'PF_TestGetPF', @actual = @pf
exec tSQLt.AssertEquals @Expected = 'PS_TestGetPF', @actual = @ps

go
