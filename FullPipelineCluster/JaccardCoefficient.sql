-- compute sum mins for (SID1,SID2) for each day
select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins 
from 
	(
		select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins 
		from 
		(
			select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from graph g1 
			inner join 
				(
					select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost 
					from 
						(
							select MilanoDate, SID1, SID2, EdgeCost from graph 
							union all 
							select MilanoDate, SID2, SID1, EdgeCost from graph
						) t
				) a 
			on 
				a.SID1 in (g1.SID1, g1.SID2) 
				and 
				a.MilanoDate = g1.MilanoDate
			where 
				(a.common_node, a.MilanoDate) in 
				(
					select t1.SID2 , t1.MilanoDate
					from 
						(
							select MilanoDate, SID1, SID2 from graph 
							union all 
							select MilanoDate, SID2, SID1 from graph
						) t1 
					where 
						t1.SID1 = g1.SID1 
						and 
						t1.MilanoDate = g1.MilanoDate
				) 
				and 
				(a.common_node, a.MilanoDate) in 
				(
					select t1.SID2, t1.MilanoDate
					from 
						(
							select MilanoDate, SID1, SID2 from graph 
							union all 
							select MilanoDate, SID2, SID1 from graph
						) t1 
					where t1.SID1 = g1.SID2 
					and 
					t1.MilanoDate = g1.MilanoDate
				)
		) b 
		group by b.MilanoDate, b.SID1, b.SID2, b.common_node
	) c 
group by c.MilanoDate, c.SID1, c.SID2

-- compute sum maxs for (SID1,SID2) for each day
select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs
from 
	(
		select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) max
		from 
		(
			select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from graph g1 
			inner join 
				(
					select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost 
					from 
						(
							select MilanoDate, SID1, SID2, EdgeCost from graph 
							union all 
							select MilanoDate, SID2, SID1, EdgeCost from graph
						) t
				) a 
			on 
				a.SID1 in (g1.SID1, g1.SID2) 
				and 
				a.MilanoDate = g1.MilanoDate
			where 
				(a.common_node, a.MilanoDate) in 
				(
					select t1.SID2 , t1.MilanoDate
					from 
						(
							select MilanoDate, SID1, SID2 from graph 
							union all 
							select MilanoDate, SID2, SID1 from graph
						) t1 
					where 
						t1.SID1 = g1.SID1 
						and 
						t1.MilanoDate = g1.MilanoDate
				) 
				or
				(a.common_node, a.MilanoDate) in 
				(
					select t1.SID2, t1.MilanoDate
					from 
						(
							select MilanoDate, SID1, SID2 from graph 
							union all 
							select MilanoDate, SID2, SID1 from graph
						) t1 
					where t1.SID1 = g1.SID2 
					and 
					t1.MilanoDate = g1.MilanoDate
				)
		) b 
		group by b.MilanoDate, b.SID1, b.SID2, b.common_node
	) c 
group by c.MilanoDate, c.SID1, c.SID2

-- Jaccard Coefficient for (node1,node2)
select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient
from
	(
		select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins 
		from 
			(
				select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins 
				from 
				(
					select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from graph g1 
					inner join 
						(
							select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost 
							from 
								(
									select MilanoDate, SID1, SID2, EdgeCost from graph 
									union all 
									select MilanoDate, SID2, SID1, EdgeCost from graph
								) t
						) a 
					on 
						a.SID1 in (g1.SID1, g1.SID2) 
						and 
						a.MilanoDate = g1.MilanoDate
					where 
						(a.common_node, a.MilanoDate) in 
						(
							select t1.SID2 , t1.MilanoDate
							from 
								(
									select MilanoDate, SID1, SID2 from graph 
									union all 
									select MilanoDate, SID2, SID1 from graph
								) t1 
							where 
								t1.SID1 = g1.SID1 
								and 
								t1.MilanoDate = g1.MilanoDate
						) 
						and 
						(a.common_node, a.MilanoDate) in 
						(
							select t1.SID2, t1.MilanoDate
							from 
								(
									select MilanoDate, SID1, SID2 from graph 
									union all 
									select MilanoDate, SID2, SID1 from graph
								) t1 
							where t1.SID1 = g1.SID2 
							and 
							t1.MilanoDate = g1.MilanoDate
						)
				) b 
				group by b.MilanoDate, b.SID1, b.SID2, b.common_node
			) c 
		group by c.MilanoDate, c.SID1, c.SID2		
	) d1
inner join
	(
		select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs
		from 
			(
				select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) max
				from 
				(
					select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from graph g1 
					inner join 
						(
							select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost 
							from 
								(
									select MilanoDate, SID1, SID2, EdgeCost from graph 
									union all 
									select MilanoDate, SID2, SID1, EdgeCost from graph
								) t
						) a 
					on 
						a.SID1 in (g1.SID1, g1.SID2) 
						and 
						a.MilanoDate = g1.MilanoDate
					where 
						(a.common_node, a.MilanoDate) in 
						(
							select t1.SID2 , t1.MilanoDate
							from 
								(
									select MilanoDate, SID1, SID2 from graph 
									union all 
									select MilanoDate, SID2, SID1 from graph
								) t1 
							where 
								t1.SID1 = g1.SID1 
								and 
								t1.MilanoDate = g1.MilanoDate
						) 
						or
						(a.common_node, a.MilanoDate) in 
						(
							select t1.SID2, t1.MilanoDate
							from 
								(
									select MilanoDate, SID1, SID2 from graph 
									union all 
									select MilanoDate, SID2, SID1 from graph
								) t1 
							where t1.SID1 = g1.SID2 
							and 
							t1.MilanoDate = g1.MilanoDate
						)
				) b 
				group by b.MilanoDate, b.SID1, b.SID2, b.common_node
			) c 
		group by c.MilanoDate, c.SID1, c.SID2
	) d2
	on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate;
