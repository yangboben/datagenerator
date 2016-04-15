-- before: create table if not exists test1(a int, b int);
-- before: truncate table test1;
-- after: drop table test1;
select count(*) from test1;
