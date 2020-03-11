SELECT family.uuid, family.created_at, family.modified_at, family.visible, family.external_id, family.family_type, family.kf_id 
FROM family JOIN participant ON family.kf_id = participant.family_id 
WHERE participant.study_id = 'REPLACE_STUDY_ID' GROUP BY family.kf_id