SELECT investigator.uuid, investigator.created_at, investigator.modified_at, investigator.visible, investigator.external_id, investigator.name, investigator.institution, investigator.kf_id 
FROM investigator JOIN study ON investigator.kf_id = study.investigator_id 
WHERE study.kf_id = 'REPLACE_STUDY_ID'