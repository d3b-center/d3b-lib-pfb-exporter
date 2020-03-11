SELECT outcome.uuid AS uuid, outcome.created_at AS created_at, outcome.modified_at AS modified_at, outcome.visible AS visible, outcome.external_id AS external_id, outcome.vital_status AS vital_status, outcome.disease_related AS disease_related, outcome.age_at_event_days AS age_at_event_days, outcome.participant_id AS participant_id, outcome.kf_id AS kf_id 
FROM participant JOIN outcome ON participant.kf_id = outcome.participant_id 
WHERE participant.study_id = 'REPLACE_STUDY_ID'