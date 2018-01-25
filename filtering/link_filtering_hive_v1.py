from pyspark import SparkContext
from pyspark.sql import HiveContext
import time

if __name__ == "__main__":
    sc = SparkContext(appName="Link Filtering Hive v1")
    hc = HiveContext(sparkContext=sc)
    tbl = hc.table("mi2mi.edges")
    tbl.registerTempTable("edges")
    
    # ties = hc.sql("select MilanoDate, sid1, sid2, 1 - (select count(distinct sid1) - 2 from edges where MilanoDate=e.MilanoDate)*(EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) + pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate))) /((select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate) * (EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) - 1)) alpha from edges e")
    ties = hc.sql("select MilanoDate, sid1, sid2, pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)) alpha from edges e order by MilanoDate, sid1, sid2")
    ties.write.format("orc").saveAsTable("mi2mi.LinkFiltering")
    alfa_value = [0.01, 0.05, 0.001]
    ties.filter(ties.alpha < 0.05).show()