Patients_lifecylce_query ="""
SELECT  member_id AS member_ids
,member_id as pk_key
,patient_address_state AS patient_address_state
,cast(assigned_pharmacy_id as varchar)
,plan_type AS plan_type
,program_eligibility AS program_eligibility
,cast(patient_id as varchar) AS patient_id
,cast(potential_pharmacy_id as varchar) AS potential_pharmacy_id
,is_minor AS is_minor
,lead_created_date AS lead_created_date
FROM patient_lifecycle pl WHERE  current_enroll_status IN ('churned','outreached')
and plan_type = 'MCAID'
"""
