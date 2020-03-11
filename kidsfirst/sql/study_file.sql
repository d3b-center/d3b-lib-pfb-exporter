SELECT study_file.uuid, study_file.latest_did, study_file.created_at, study_file.modified_at, study_file.visible, study_file.external_id, study_file.study_id, study_file.availability, study_file.data_type, study_file.file_format, study_file.kf_id 
FROM study_file 
WHERE study_file.study_id = 'REPLACE_STUDY_ID'