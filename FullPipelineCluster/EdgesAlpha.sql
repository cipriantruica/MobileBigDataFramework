select MilanoDate, sid1, sid2, EdgeCost,
	pow(1 - EdgeCost/
		(select sum(EdgeCost) node_strength 
			from (select Date MilanoDate, SID1, SID2, sum(DIS) EdgeCost 
from 
(
	select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 
		union all 
	select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2
) 
group by Date, SID1, SID2 order by Date, SID1, SID2)
			where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), 
	(select count(distinct sid1) - 1 from (select Date MilanoDate, SID1, SID2, sum(DIS) EdgeCost 
from 
(
	select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 
		union all 
	select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2
) 
group by Date, SID1, SID2 order by Date, SID1, SID2)
 where MilanoDate=e.MilanoDate)) alpha 
from (select Date MilanoDate, SID1, SID2, sum(DIS) EdgeCost 
from 
(
	select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 
		union all 
	select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2
) 
group by Date, SID1, SID2 order by Date, SID1, SID2)
 e 
	where sid1 != sid2 
order by MilanoDate, sid1, sid2