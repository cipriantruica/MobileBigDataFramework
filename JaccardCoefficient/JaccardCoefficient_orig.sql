select node1, node2, 
	(select sum(a.w) from
		(select min(t.weight) w
			from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t
			where
				t.node1 in (g1.node1, g1.node2) 
				and
				t.node2 in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1
							intersect
							select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
			group by t.node2) a
	) min_weight,
	(select sum(a.w) from
		(select max(t.weight) w
			from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t
			where
				t.node1 in (g1.node1, g1.node2) 
				and
				t.node2 in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1
								union all
								select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
		group by t.node2) a
	) max_weight,
	(select sum(a.w) from
		(select min(t.weight) w
			from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t
			where
				t.node1 in (g1.node1, g1.node2) 
				and
				t.node2 in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1
							intersect
							select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
			group by t.node2) a
	) /
	(select sum(a.w) from
		(select max(t.weight) w
			from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t
			where
				t.node1 in (g1.node1, g1.node2) 
				and
				t.node2 in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1
								union all
								select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)
		group by t.node2) a
	) jaccard_coefficient
from graph g1;


