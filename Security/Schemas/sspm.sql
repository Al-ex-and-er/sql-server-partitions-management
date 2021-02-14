--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
if not exists (select 1 from sys.schemas where name = 'sspm')
begin
  EXEC ('CREATE SCHEMA sspm')
end
go
