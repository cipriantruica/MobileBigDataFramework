-- computes the sum of min values for (node1, node2)
select c.node1, c.node2, sum(mins) sum_mins
from 
(
	select b.node1, b.node2, b.common_node, min(b.weight) mins
	from 
	(
		select g1.node1, g1.node2, a.common_node, a.weight
		from graph g1
			inner join 
				(select t.node2 common_node, t.node1, t.weight
					from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t) a
					on a.node1 in (g1.node1, g1.node2) 
			where 
				a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1)
				and 			
				a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
	) b
	group by b.node1, b.node2, b.common_node
) c
group by c.node1, c.node2;

-- computes the sum of max values for (node1, node2)
select c.node1, c.node2, sum(maxs) sum_maxs
from 
(
	select b.node1, b.node2, b.common_node, max(b.weight) maxs
	from 
	(
		select g1.node1, g1.node2, a.common_node, a.weight
		from graph g1
			inner join 
				(select t.node2 common_node, t.node1, t.weight
					from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t) a
					on a.node1 in (g1.node1, g1.node2) 
			where 
				a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1)
				or 			
				a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
	) b
	group by b.node1, b.node2, b.common_node
) c
group by c.node1, c.node2;

-- Jaccard Coefficient for (node1,node2)
select d1.node1, d2.node2, d1.sum_mins/d2.sum_maxs jaccard_coefficient
from
	(
		select c.node1, c.node2, sum(mins) sum_mins
		from 
		(
			select b.node1, b.node2, b.common_node, min(b.weight) mins
			from 
			(
				select g1.node1, g1.node2, a.common_node, a.weight
				from graph g1
					inner join 
						(select t.node2 common_node, t.node1, t.weight
							from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t) a
							on a.node1 in (g1.node1, g1.node2) 
					where 
						a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1)
						and 			
						a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
			) b
			group by b.node1, b.node2, b.common_node
		) c
		group by c.node1, c.node2
	) d1
	inner join
	(
		select c.node1, c.node2, sum(max) sum_maxs
		from 
		(
			select b.node1, b.node2, b.common_node, max(b.weight) max
			from 
			(
				select g1.node1, g1.node2, a.common_node, a.weight
				from graph g1
					inner join 
						(select t.node2 common_node, t.node1, t.weight
							from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t) a
							on a.node1 in (g1.node1, g1.node2) 
					where 
						a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1)
						or 			
						a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
			) b
			group by b.node1, b.node2, b.common_node
		) c
		group by c.node1, c.node2
	) d2
	on d1.node1 = d2.node1 and d1.node2=d2.node2;