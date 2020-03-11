SELECT family.uuid AS uuid, family.created_at AS created_at, family.modified_at AS modified_at, family.visible AS visible, family.external_id AS external_id, family.family_type AS family_type, family.kf_id AS kf_id 
FROM family JOIN participant ON family.kf_id = participant.family_id 
WHERE participant.study_id = 'REPLACE_STUDY_ID' GROUP BY family.kf_id