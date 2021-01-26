SELECT outcome.uuid, outcome.created_at, outcome.modified_at, outcome.visible, outcome.external_id, outcome.vital_status, outcome.disease_related, outcome.age_at_event_days, outcome.participant_id, outcome.kf_id 
FROM participant JOIN outcome ON participant.kf_id = outcome.participant_id 
WHERE participant.study_id = 'REPLACE_STUDY_ID'