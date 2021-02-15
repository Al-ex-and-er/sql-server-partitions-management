--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
--DROP TABLE sspm.PS_Layout
CREATE TABLE sspm.PS_Layout
(
  LayoutName varchar(50) not null,
  PointFrom sql_variant null,
  PointTo sql_variant null,
  FGName sysname not null, 
  constraint UQ_sspm_PS_Layout unique(LayoutName, PointFrom)
)

/*
INSERT INTO sspm.PS_Layout VALUES
('L1', 1, 10, 'FG1'),
('L1', 10, 20, 'FG2')

INSERT INTO sspm.PS_Layout VALUES
('L2', cast('2020-01-01' as date), cast('2020-01-02' as date), 'FG1'),
('L2', cast('2020-01-02' as date), cast('2020-01-03' as date), 'FG1')

select *, SQL_VARIANT_PROPERTY(PointFrom, 'BaseType')
from sspm.PS_Layout

--truncate table sspm.PS_Layout
*/
