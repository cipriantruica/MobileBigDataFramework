select a.community, b.community, count(b.sid1)
from mi2mi.louvaincommunity a inner join mi2mi.louvaincommunity b
on
    a.level = b.level
    and a.sid1 = b.sid1
    and a.milanodate = b.milanodate
    and a.edgecostfactor = b.edgecostfactor
where
    a.level = 0
    and a.alphathreshold = 1000
    and b.alphathreshold = 10
    and a.milanodate='2013-11-08'
    and a.edgecostfactor = 1000000000000
group by a.community, b.community
order by a.community, b.community;

