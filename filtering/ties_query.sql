-- entire formula for alpha
select sid1, sid2, 
	1 - (select count(distinct sid1) - 2 from edges where MilanoDate=e.MilanoDate)*(
		EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) +
		pow(
			1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), 
			(select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)
		)
	) /
	(
		(select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate) * 
		(EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) - 1)
	)
from edges e 
	where MilanoDate='2013-11-01'

-- simplified
select sid1, sid2, 
	pow(
		1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), 
		(select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)
	)
from edges e 
	where MilanoDate='2013-11-01'
