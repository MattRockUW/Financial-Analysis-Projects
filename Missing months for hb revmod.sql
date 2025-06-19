
; with allocationrange as 
(
select allocation_type, min(actual_dt) as MinGLPostDate, max(actual_dt) as MaxGLPostDate
from mart_load_uwhealth.strata_revmod_hb_gl_totals revmod
join [Modeled_UWHealth].[DIM_DATE] dd on revmod.fy = dd.mid_year_fiscal_year_nbr
and revmod.fp = dd.mid_year_fiscal_month_nbr
where day_of_month_nbr = 1
group by allocation_type
),

filleddates as (
select allocation_type, calendar_year_nm, mid_year_fiscal_year_nbr, mid_year_fiscal_month_nbr 
from allocationrange ar
join [Modeled_UWHealth].[DIM_DATE] dd on dd.day_of_month_nbr = '1'
and dd.actual_dt between ar.MinGLPostDate and ar.MaxGLPostDate
),

monthlygl as (
select cy, fy, fp, allocation_type, sum(gl_amount) as 'glamount', count(*) as 'entrycount', 
row_number() OVER (partition by allocation_type order by fy, fp) as rownum from mart_load_uwhealth.strata_revmod_hb_gl_totals
group by cy, fy, fp,  allocation_type
)

select * from filleddates fd
left join monthlygl on monthlygl.allocation_type = fd.allocation_type
and monthlygl.fy = fd.mid_year_fiscal_year_nbr
and monthlygl.fp = fd.mid_year_fiscal_month_nbr
where fd.allocation_type not like 'CY%Est%Risk' and fd.allocation_type not in ('MC Reserve', 'OPO Reserve')
and entrycount is null
order by 1,3,4
