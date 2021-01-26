SELECT study.uuid, study.created_at, study.modified_at, study.visible, study.data_access_authority, study.external_id, study.version, study.name, study.short_name, study.attribution, study.release_status, study.investigator_id, study.kf_id 
FROM study 
WHERE study.kf_id = 'REPLACE_STUDY_ID'