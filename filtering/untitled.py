from pyspark import SparkContext
from pyspark.sql import HiveContext
import time
from graphframes import GraphFrame

# Strong and weak ties
def filtering(alpha_thr):



if __name__ == "__main__":
    sc = SparkContext(appName="Edges Filtering Hive v1")
    hc = HiveContext(sparkContext=sc)
    tbl = hc.table("mi2mi.edges")
    tbl.registerTempTable("edges")
    edgesDF = hc.sql("select SID1, SID2, EdgeCost from edges where MilanoDate='2013-11-01'")
    edgesDF.show()

    v = hc.sql("select distinct SID1 id from edges where MilanoDate='2013-11-01' order by SID1")
    e = hc.sql("select SID1 src, SID2 dst, EdgeCost cost from edges where MilanoDate='2013-11-01'")
    d = hc.sql("select sid1, sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate='2013-11-01' group by sid1")
    g = GraphFrame(v, e)

    g.vertices.show()
    g.edges.show()

    
    d = hc.sql("select sid1, sid2, 1 - (select count(distinct sid1) - 2 from edges where MilanoDate=e.MilanoDate)*(EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) + pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate))) /((select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate) * (EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) - 1)) alpha from edges e where MilanoDate='2013-11-01'")
    d.show()
