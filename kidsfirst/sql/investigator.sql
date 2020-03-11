SELECT investigator.uuid AS uuid, investigator.created_at AS created_at, investigator.modified_at AS modified_at, investigator.visible AS visible, investigator.external_id AS external_id, investigator.name AS name, investigator.institution AS institution, investigator.kf_id AS kf_id 
FROM investigator JOIN study ON investigator.kf_id = study.investigator_id 
WHERE study.kf_id = 'REPLACE_STUDY_ID'