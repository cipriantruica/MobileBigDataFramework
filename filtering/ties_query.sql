-- simplified for the entire data
-- project the Date (MilanoDate), Square ID1 (SID1), Square ID2 (SID2) and alpha
select MilanoDate, sid1, sid2, 
	-- this implements equation (1) from your paper
	pow(
			-- get the egde cost for nodes SID1 and SID2
			1 - EdgeCost/
			-- compute the sum of weights (node_strength) for node SID1 
			-- for the date from the main query 
			-- (there are multiple dates in the table and I want to make sure that I compute the sum for a specific date)
			-- also corelate the SID1 from the main query with the SID1 for the nested query
			-- also I make sure that SID1 != SID2 to remove the edges that selfreference.
				(select sum(EdgeCost) node_strength 
					from edges 
					where 
						sid1 != sid2 and 
						MilanoDate=e.MilanoDate and 
						sid1 = e.sid1 
					group by sid1), 
			-- compute the number of nodes for a specific date
			(select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)
		) alpha 
from edges e 
-- remove the edges for that self reference themselfs
where sid1 != sid2
order by MilanoDate, sid1, sid2

-- Ignore this part - this where some tests I done before simplifications
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

-- simplified for one day
select sid1, sid2, 
	pow(
		1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), 
		(select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)
	)
from edges e 
	where MilanoDate='2013-11-01'

