with monthlygl as (
select cy, fy, fp, allocation_type, sum(gl_amount) as 'glamount', count(*) as 'entrycount', 
row_number() OVER (partition by allocation_type order by fy, fp) as rownum from mart_load_uwhealth.strata_revmod_hb_gl_totals
where allocation_type not like 'CY%Est%Risk' and allocation_type not in ('MC Reserve', 'OPO Reserve')
group by cy, fy, fp,  allocation_type
),

stddev as (
select distinct allocation_type, 
CASE WHEN allocation_type in ('GHC Cap', 'MA DSH Supplement','Quartz Cap') THEN 3
WHEN allocation_type in ('GME') THEN 2.5
WHEN allocation_type in ('MA HMO Payment - IP', 'MA HMO Payment - OP') THEN 2
ELSE 1 END as 'stddev_variable'
from monthlygl
),

math as (
select monthlygl.*, stddev.stddev_variable, 
CASE WHEN monthlygl.allocation_type = 'Quartz Cap' THEN stdev(glamount) OVER (partition by monthlygl.allocation_type, fy order by fp rows between 12 preceding and  1 preceding)
ELSE stdev(glamount) OVER (partition by monthlygl.allocation_type order by fy, fp rows between 12 preceding and  1 preceding) END as 'rollingglstddev', 
CASE WHEN monthlygl.allocation_type = 'Quartz Cap' THEN avg(glamount) OVER (partition by monthlygl.allocation_type, fy order by fp rows between 12 preceding and 1 preceding)
ELSE avg(glamount) OVER (partition by monthlygl.allocation_type order by fy, fp rows between 12 preceding and 1 preceding) END as 'rollingglavg' 

from monthlygl 
join stddev on monthlygl.allocation_type = stddev.allocation_type
)

select *, 
CASE WHEN rollingglstddev = 0 THEN 0 ELSE abs(glamount - rollingglavg) / rollingglstddev END as 'StdDeviation'
from math
--where rownum > 2 and stddev_variable < CASE WHEN rollingglstddev = 0 THEN 0 ELSE abs(glamount - rollingglavg) / rollingglstddev END 
order by allocation_type, fy, fp
