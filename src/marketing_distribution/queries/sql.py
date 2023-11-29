Patients_lifecylce_query ="""
SELECT  member_id AS member_ids
member_id as pk_key,
,patient_address_state AS patient_address_state
,assigned_pharmacy_id AS assigned_pharmacy_id
,plan_type AS plan_type
,program_eligibility AS program_eligibility
,patient_id AS patient_id
,potential_pharmacy_id AS potential_pharmacy_id
,is_minor AS is_minor
,lead_created_date AS lead_created_date
FROM patient_lifecycle pl WHERE  current_enroll_status NOT IN ('active','not_interested')
"""
