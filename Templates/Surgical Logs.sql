

select 
olc.log_id, 
olc.surgery_date, 
olc.room_id,
A.PRIN_GL_BUILDING,
locat.location_nm,
olc.num_of_panels,
eocap.or_proc_id,
eopc.proc_name, 
--eflb.scheduled_in_or_dttm,
--eflb.scheduled_out_or_dttm,
--eflb.in_or_dttm,
--eflb.out_or_dttm,
eflb.minutes_scheduled_in_or,
eflb.minutes_in_or, 
epoalc.pat_enc_csn_id,
epoalc.pat_id, 
epoalc.or_link_csn,
epoalc.or_link_inp_id,
ADMIT_DEPARTMENT_ID,
ADMIT_DEPARTMENT,
PRIN_CPT_CODE,
eflb.inpatient_yn,
PRIN_ICD10_PROCEDURE_CODE,
PRIN_ICD10_DIAGNOSIS_CODE,
ADMIT_ICD10_DIAGNOSIS_CODE,
ADMIT.name as AdmitProviderName,
ATTEND.name as AttendingProviderName,
surg.name as SurgeonName,
OR_CASES,
MSDRG_CODE,
MSDRG_CODE_GROUP,
MEDSURG_CODE,
MSDRG_COST_WGT_NBR,
APRDRG_CODE,
APRDRG_ROM,
APRDRG_SOI,
MSMDC,
A.INS_PLAN_1_GL_PAYOR_GROUP_1,
INSURANCE_PLAN_1,
efmp.FIXED_NON_FIXED_FLG,	
efmp.PAYOR_RISK_FLG,	
efmp.FIN_MODEL_PAYOR_GRP,
ADMIT_DT,
DISCHARGE_DT,
COALESCE(A.CHARGE_AMOUNT, 0) + COALESCE(M_PB.CHARGE_AMOUNT, 0) CHARGE_AMOUNT,
COALESCE(A.ESTIMATED_REIMBURSEMENT, 0) + COALESCE(M_PB.ESTIMATED_REIMBURSEMENT, 0) ESTIMATED_REIMBURSEMENT,
COALESCE(A.DIRECT_COST, 0) + COALESCE(M_PB.DIRECT_COST, 0) DIRECT_COST,
COALESCE(A.VAR_INDIRECT_COST, 0) + COALESCE(M_PB.VAR_INDIRECT_COST, 0) VAR_INDIRECT_COST,
COALESCE(A.FIXED_INDIRECT_COST, 0) + COALESCE(M_PB.FIXED_INDIRECT_COST, 0) FIXED_INDIRECT_COST,
--Contribution Margin Calc
COALESCE(A.ESTIMATED_REIMBURSEMENT, 0)  + COALESCE(M_PB.ESTIMATED_REIMBURSEMENT , 0) 
- COALESCE(A.DIRECT_COST, 0)  - COALESCE(M_PB.DIRECT_COST, 0) 
- COALESCE(A.VAR_INDIRECT_COST, 0)  - COALESCE(M_PB.VAR_INDIRECT_COST, 0)  CONTR_MARGIN,
COALESCE(A.ESTIMATED_REIMBURSEMENT, 0)  + COALESCE(M_PB.ESTIMATED_REIMBURSEMENT, 0) 
- COALESCE(A.DIRECT_COST, 0)  - COALESCE(M_PB.DIRECT_COST, 0) 
- COALESCE(A.VAR_INDIRECT_COST, 0)  - COALESCE(M_PB.VAR_INDIRECT_COST, 0) 
- COALESCE(A.FIXED_INDIRECT_COST, 0)  - COALESCE(M_PB.FIXED_INDIRECT_COST, 0) OP_MARGIN
from [Source_UWHealth].EPIC_OR_LOG_CUR olc
join [Source_UWHealth].EPIC_OR_case_ALL_PROC_CUR eocap on olc.log_id = eocap.or_Case_id
join [Source_UWHealth].EPIC_OR_PROC_CUR eopc on eocap.or_proc_id = eopc.or_proc_id
join [Source_UWHealth].EPIC_PAT_OR_ADM_LINK_CUR epoalc on epoalc.log_id = eocap.or_case_id  
join [Source_UWHealth].EPIC_F_LOG_BASED_CUR eflb on eflb.log_id = olc.log_id
join [Modeled_UWHealth].DIM_LOCATION_CUR locat on locat.loc_id = olc.loc_id
join [Mart_UWHealth].[STRATA_COST_ACCOUNT_HB] A on a.pat_enc_csn_id=epoalc.or_link_csn and a.pat_enc_csn_id is not null
join UDD_UWHealth.EA_FIN_MODEL_PAYOR_GRP_MAP efmp on efmp.INS_PLAN_1_GL_PAYOR_GROUP_1 = A.INS_PLAN_1_GL_PAYOR_GROUP_1 
	and efmp.FIXED_NON_FIXED_FLG = a.FIXED_NON_FIXED_FLG
LEFT JOIN --Joining on Matched PB
(SELECT 
MATCHED_HB_STRATA_ENCOUNTER,
SUM(CHARGE_AMOUNT) CHARGE_AMOUNT,
SUM(ESTIMATED_REIMBURSEMENT) ESTIMATED_REIMBURSEMENT, 
SUM(DIRECT_COST) DIRECT_COST,
SUM(VAR_INDIRECT_COST) VAR_INDIRECT_COST,
SUM(FIXED_INDIRECT_COST) FIXED_INDIRECT_COST
FROM 
[Mart_UWHealth].[STRATA_COST_ACCOUNT_PB]
WHERE MATCHED_HB_STRATA_ENCOUNTER IS NOT NULL
GROUP BY MATCHED_HB_STRATA_ENCOUNTER) M_PB ON A.STRATA_ENCOUNTER_REC_NBR = M_PB.MATCHED_HB_STRATA_ENCOUNTER

join [source_uwhealth].epic_CLARITY_EMP_cur admit on admit.prov_id = ADMIT_PROVIDER_ID
join [source_uwhealth].epic_CLARITY_EMP_cur ATTEND on ATTEND.prov_id = ATTEND_PROVIDER_ID
join [source_uwhealth].epic_CLARITY_EMP_cur surg on surg.prov_id = PRINCIPAL_SURGEON_ID
where eflb.primary_procedure_id in ('137952', '138672', '138816', '138960', '139104', '139392', '1728', '300027', '300031', '300036', '300043', '339042', '339044', '339558', '339626', '339628', '4838', '4900', '4925', '5104', '5120', '5142', '5150', '5151', '5368', '5379', '5442', '5462', '5510', '5519', '6066', '6071', '6072', '6367', '6446', '6448', '6450', '6725', '7060', '7069', '7220', '7269', '7270', '7275', '7365', '7366', '7367', '7368', '7378', '7596', '7635', '7670', '7677', '81936', '82080', '82224', '82368', '92448', '92592', '93744', '94032', '94176', '94320', '94464', '94752', '94896', '95040')
and eflb.primary_procedure_id = eocap.or_proc_id 
and olc.surgery_date >= '01/01/2024'
and olc.loc_id not in ('110037000')
--and olc.log_id = '1075438'
order by surgery_date desc

