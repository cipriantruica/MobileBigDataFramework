-- posgres
create table edges(milanodate date, SID1 int, SID2 int, EdgeCost float);
alter table edges add constraint pk_edges primary key (milanodate, SID1, SID2);

create table linkfiltering as 
    select MilanoDate, sid1, sid2, pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)) alpha from edges e where sid1 != sid2;

alter table linkfiltering add constraint pk_linkfiltering primary key (milanodate, SID1, SID2); 

create table jaccardcoefficient as 
    select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from 
    (
        select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node ) b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2
    ) d1 
    inner join 
    (
        select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node) ) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2
    ) d2 
    on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate;


alter table jaccardcoefficient add constraint pk_jaccardcoefficient primary key (milanodate, SID1, SID2); 

insert into edges values('2013-11-01', 1, 2, 2);
insert into edges values('2013-11-01', 1, 3, 5);
insert into edges values('2013-11-01', 1, 4, 6);
insert into edges values('2013-11-01', 2, 4, 2);
insert into edges values('2013-11-01', 3, 4, 3);
insert into edges values('2013-11-01', 4, 5, 1);
insert into edges values('2013-11-01', 4, 6, 3);
insert into edges values('2013-11-01', 5, 6, 4);
insert into edges values('2013-11-01', 5, 8, 2);
insert into edges values('2013-11-01', 5, 7, 5);
insert into edges values('2013-11-01', 6, 7, 2);
insert into edges values('2013-11-01', 6, 8, 3);
insert into edges values('2013-11-01', 7, 8, 1);
insert into edges values('2013-11-01', 7, 9, 3);

insert into edges values('2013-11-02', 1, 2, 2);
insert into edges values('2013-11-02', 1, 3, 5);
insert into edges values('2013-11-02', 1, 4, 6);
insert into edges values('2013-11-02', 2, 4, 2);
insert into edges values('2013-11-02', 3, 4, 3);
insert into edges values('2013-11-02', 4, 5, 1);
insert into edges values('2013-11-02', 4, 6, 3);
insert into edges values('2013-11-02', 5, 6, 4);
insert into edges values('2013-11-02', 5, 8, 2);
insert into edges values('2013-11-02', 5, 7, 5);
insert into edges values('2013-11-02', 6, 7, 2);
insert into edges values('2013-11-02', 6, 8, 3);
insert into edges values('2013-11-02', 7, 8, 1);
insert into edges values('2013-11-02', 7, 9, 3);

COPY edges FROM '/home/ciprian/Desktop/research/DATA_SETS/edges/000000_0' WITH (FORMAT csv);
-- oracle
create table edges(milanodate date, SID1 int, SID2 int, EdgeCost double precision);

insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 1, 2, 2);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 1, 3, 5);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 1, 4, 6);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 2, 4, 2);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 3, 4, 3);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 4, 5, 1);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 4, 6, 3);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 5, 6, 4);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 5, 8, 2);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 5, 7, 5);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 6, 7, 2);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 6, 8, 3);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 7, 8, 1);
insert into edges values(to_date('2013-11-01', 'YYYY-MM-DD'), 7, 9, 3);

insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 1, 2, 2);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 1, 3, 5);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 1, 4, 6);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 2, 4, 2);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 3, 4, 3);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 4, 5, 1);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 4, 6, 3);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 5, 6, 4);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 5, 8, 2);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 5, 7, 5);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 6, 7, 2);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 6, 8, 3);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 7, 8, 1);
insert into edges values(to_date('2013-11-02', 'YYYY-MM-DD'), 7, 9, 3);

commit;
