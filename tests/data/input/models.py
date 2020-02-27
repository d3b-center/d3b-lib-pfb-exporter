# coding: utf-8
from sqlalchemy import ARRAY, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AliasGroup(Base):
    __tablename__ = 'alias_group'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))


class CavaticaApp(Base):
    __tablename__ = 'cavatica_app'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_cavatica_app_id = Column(Text)
    name = Column(Text)
    revision = Column(Integer)
    github_commit_url = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))


class Family(Base):
    __tablename__ = 'family'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    family_type = Column(Text)


class GenomicFile(Base):
    __tablename__ = 'genomic_file'

    uuid = Column(UUID, unique=True)
    latest_did = Column(UUID, nullable=False)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    data_type = Column(Text)
    file_format = Column(Text)
    is_harmonized = Column(Boolean)
    reference_genome = Column(Text)
    controlled_access = Column(Boolean)
    availability = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    paired_end = Column(Integer)


class Investigator(Base):
    __tablename__ = 'investigator'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    name = Column(Text)
    institution = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))


class ReadGroup(Base):
    __tablename__ = 'read_group'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    flow_cell = Column(Text)
    lane_number = Column(Float(53))
    quality_scale = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))


class SequencingCenter(Base):
    __tablename__ = 'sequencing_center'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    name = Column(Text, nullable=False, unique=True)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))


class ReadGroupGenomicFile(Base):
    __tablename__ = 'read_group_genomic_file'
    __table_args__ = (
        UniqueConstraint('read_group_id', 'genomic_file_id'),
    )

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    read_group_id = Column(ForeignKey('read_group.kf_id'), nullable=False)
    genomic_file_id = Column(ForeignKey('genomic_file.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    external_id = Column(Text)

    genomic_file = relationship('GenomicFile')
    read_group = relationship('ReadGroup')


class SequencingExperiment(Base):
    __tablename__ = 'sequencing_experiment'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text, nullable=False)
    experiment_date = Column(DateTime)
    experiment_strategy = Column(Text, nullable=False)
    library_name = Column(Text)
    library_strand = Column(Text)
    is_paired_end = Column(Boolean, nullable=False)
    platform = Column(Text, nullable=False)
    instrument_model = Column(Text)
    max_insert_size = Column(Integer)
    mean_insert_size = Column(Float(53))
    mean_depth = Column(Float(53))
    total_reads = Column(Integer)
    mean_read_length = Column(Float(53))
    sequencing_center_id = Column(ForeignKey('sequencing_center.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    library_prep = Column(Text)
    library_selection = Column(Text)

    sequencing_center = relationship('SequencingCenter')


class Study(Base):
    __tablename__ = 'study'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    data_access_authority = Column(Text, nullable=False)
    external_id = Column(Text, nullable=False)
    version = Column(Text)
    name = Column(Text)
    short_name = Column(Text)
    attribution = Column(Text)
    release_status = Column(Text)
    investigator_id = Column(ForeignKey('investigator.kf_id'))
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    study_code = Column(Text, nullable=False, unique=True)

    investigator = relationship('Investigator')


class Task(Base):
    __tablename__ = 'task'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_task_id = Column(UUID)
    name = Column(Text)
    cavatica_app_id = Column(ForeignKey('cavatica_app.kf_id'))
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))

    cavatica_app = relationship('CavaticaApp')


class Participant(Base):
    __tablename__ = 'participant'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    family_id = Column(ForeignKey('family.kf_id'))
    is_proband = Column(Boolean)
    race = Column(Text)
    ethnicity = Column(Text)
    gender = Column(Text)
    study_id = Column(ForeignKey('study.kf_id'), nullable=False)
    alias_group_id = Column(ForeignKey('alias_group.kf_id'))
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    affected_status = Column(Boolean)
    diagnosis_category = Column(Text)
    taxonomy = Column(Text)

    alias_group = relationship('AliasGroup')
    family = relationship('Family')
    study = relationship('Study')


class SequencingExperimentGenomicFile(Base):
    __tablename__ = 'sequencing_experiment_genomic_file'
    __table_args__ = (
        UniqueConstraint('sequencing_experiment_id', 'genomic_file_id'),
    )

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    sequencing_experiment_id = Column(ForeignKey('sequencing_experiment.kf_id'), nullable=False)
    genomic_file_id = Column(ForeignKey('genomic_file.kf_id'), nullable=False)
    external_id = Column(Text)
    kf_id = Column(String(11), primary_key=True)

    genomic_file = relationship('GenomicFile')
    sequencing_experiment = relationship('SequencingExperiment')


class StudyFile(Base):
    __tablename__ = 'study_file'

    uuid = Column(UUID, unique=True)
    latest_did = Column(UUID, nullable=False)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    study_id = Column(ForeignKey('study.kf_id'), nullable=False)
    availability = Column(Text)
    data_type = Column(Text)
    file_format = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))

    study = relationship('Study')


class TaskGenomicFile(Base):
    __tablename__ = 'task_genomic_file'
    __table_args__ = (
        UniqueConstraint('genomic_file_id', 'task_id', 'is_input'),
    )

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    genomic_file_id = Column(ForeignKey('genomic_file.kf_id'), nullable=False)
    task_id = Column(ForeignKey('task.kf_id'), nullable=False)
    is_input = Column(Boolean, nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))

    genomic_file = relationship('GenomicFile')
    task = relationship('Task')


class Biospeciman(Base):
    __tablename__ = 'biospecimen'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_sample_id = Column(Text)
    external_aliquot_id = Column(Text)
    source_text_tissue_type = Column(Text)
    composition = Column(Text)
    source_text_anatomical_site = Column(Text)
    age_at_event_days = Column(Integer)
    source_text_tumor_descriptor = Column(Text)
    shipment_origin = Column(Text)
    analyte_type = Column(Text, nullable=False)
    concentration_mg_per_ml = Column(Float(53))
    volume_ul = Column(Float(53))
    shipment_date = Column(DateTime)
    uberon_id_anatomical_site = Column(Text)
    ncit_id_tissue_type = Column(Text)
    ncit_id_anatomical_site = Column(Text)
    spatial_descriptor = Column(Text)
    participant_id = Column(ForeignKey('participant.kf_id'), nullable=False)
    sequencing_center_id = Column(ForeignKey('sequencing_center.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    dbgap_consent_code = Column(Text)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    consent_type = Column(Text)
    method_of_sample_procurement = Column(Text)
    duo_ids = Column(ARRAY(Text()))

    participant = relationship('Participant')
    sequencing_center = relationship('SequencingCenter')


class Diagnosi(Base):
    __tablename__ = 'diagnosis'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    source_text_diagnosis = Column(Text)
    diagnosis_category = Column(Text)
    source_text_tumor_location = Column(Text)
    age_at_event_days = Column(Integer)
    mondo_id_diagnosis = Column(Text)
    icd_id_diagnosis = Column(Text)
    uberon_id_tumor_location = Column(Text)
    ncit_id_diagnosis = Column(Text)
    spatial_descriptor = Column(Text)
    participant_id = Column(ForeignKey('participant.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))

    participant = relationship('Participant')


class FamilyRelationship(Base):
    __tablename__ = 'family_relationship'
    __table_args__ = (
        UniqueConstraint('participant1_id', 'participant2_id', 'participant1_to_participant2_relation', 'participant2_to_participant1_relation'),
    )

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    participant1_id = Column(ForeignKey('participant.kf_id'), nullable=False)
    participant2_id = Column(ForeignKey('participant.kf_id'), nullable=False)
    participant1_to_participant2_relation = Column(Text, nullable=False)
    participant2_to_participant1_relation = Column(Text)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    source_text_notes = Column(Text)

    participant1 = relationship('Participant', primaryjoin='FamilyRelationship.participant1_id == Participant.kf_id')
    participant2 = relationship('Participant', primaryjoin='FamilyRelationship.participant2_id == Participant.kf_id')


class Outcome(Base):
    __tablename__ = 'outcome'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    vital_status = Column(Text)
    disease_related = Column(Text)
    age_at_event_days = Column(Integer)
    participant_id = Column(ForeignKey('participant.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))

    participant = relationship('Participant')


class Phenotype(Base):
    __tablename__ = 'phenotype'

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    external_id = Column(Text)
    source_text_phenotype = Column(Text)
    hpo_id_phenotype = Column(Text)
    snomed_id_phenotype = Column(Text)
    observed = Column(Text)
    age_at_event_days = Column(Integer)
    participant_id = Column(ForeignKey('participant.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))

    participant = relationship('Participant')


class BiospecimenDiagnosi(Base):
    __tablename__ = 'biospecimen_diagnosis'
    __table_args__ = (
        UniqueConstraint('diagnosis_id', 'biospecimen_id'),
    )

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    diagnosis_id = Column(ForeignKey('diagnosis.kf_id'), nullable=False)
    biospecimen_id = Column(ForeignKey('biospecimen.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    external_id = Column(Text)

    biospecimen = relationship('Biospeciman')
    diagnosis = relationship('Diagnosi')


class BiospecimenGenomicFile(Base):
    __tablename__ = 'biospecimen_genomic_file'
    __table_args__ = (
        UniqueConstraint('genomic_file_id', 'biospecimen_id'),
    )

    uuid = Column(UUID, unique=True)
    created_at = Column(DateTime, index=True)
    modified_at = Column(DateTime)
    genomic_file_id = Column(ForeignKey('genomic_file.kf_id'), nullable=False)
    biospecimen_id = Column(ForeignKey('biospecimen.kf_id'), nullable=False)
    kf_id = Column(String(11), primary_key=True)
    visible = Column(Boolean, nullable=False, server_default=text("true"))
    external_id = Column(Text)

    biospecimen = relationship('Biospeciman')
    genomic_file = relationship('GenomicFile')
