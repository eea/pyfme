import rn3

ds = rn3.DatasetModel()
ds.from_url(
    dataset_id=60425,
    api_key="ApiKey 7fee1baa-f8f9-49bf-a21b-227749c961d5",
    base_url=r"https://api.reportnet.europa.eu",
)

sql_cmd = ds.sqlalchemy_generate_models(schema_name="annex_XXIV")

print(sql_cmd)

## GENERATED FROM ##

from sqlalchemy import (
    Column,
    ForeignKey,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.mssql import NVARCHAR, TEXT

Base = declarative_base()


class ReportNet3HistoricReleases(Base):
    __tablename__ = "ReportNet3HistoricReleases"
    __table_args__ = {"schema": "metadata"}

    Id = Column(Integer, primary_key=True)
    countryCode = Column(NVARCHAR(2), nullable=False)
    ReportNet3DataflowId = Column(Integer, nullable=False)
    ReportNet3EuDatasetId = Column(Integer, nullable=True)
    dataURL = Column(NVARCHAR(100), nullable=False)
    releaseDate = Column(DateTime, nullable=False)
    isLatestRelease = Column(Boolean, nullable=False)
    fmeJobId = Column(Boolean, nullable=True)
    fmeJobStartedAt = Column(DateTime, nullable=True)
    fmeSuccess = Column(Boolean, nullable=True)
    fmeErrorLog = Column(NVARCHAR(1000), nullable=True)


class PaMs(Base):
    __tablename__ = "pams"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Id_field = Column(Integer, primary_key=True, nullable=True)
    Title = Column(NVARCHAR(500), nullable=True)
    TitleNational = Column(NVARCHAR(500), nullable=False)
    IsGroup = Column(Integer, nullable=True)
    ListOfSinglePams = Column(NVARCHAR(500), nullable=False)
    ShortDescription = Column(TEXT, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Table_1(Base):
    __tablename__ = "table_1"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    GeographicalCoverage = Column(Integer, nullable=True)
    GHGAffected = Column(NVARCHAR(500), nullable=False)
    QuantifiedObjective = Column(NVARCHAR(500), nullable=True)
    AssessmentContribution = Column(TEXT, nullable=True)
    TypePolicyInstrument = Column(NVARCHAR(500), nullable=False)
    OtherPolicyInstrument = Column(NVARCHAR(500), nullable=False)
    UnionPolicy = Column(Integer, nullable=True)
    UnionPolicyList = Column(Integer, nullable=False)
    PaMRelateAirQuality = Column(Integer, nullable=False)
    StatusImplementation = Column(Integer, nullable=False)
    ImplementationPeriodStart = Column(Integer, nullable=False)
    ImplementationPeriodFinish = Column(Integer, nullable=False)
    ImplementationPeriodComment = Column(TEXT, nullable=False)
    ProjectionsScenario = Column(Integer, nullable=False)
    Comments = Column(TEXT, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    partNDC = Column(Integer, nullable=True)
    CommentQuantifiedObjective = Column(TEXT, nullable=False)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class SectorObjectives(Base):
    __tablename__ = "sectorobjectives"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Id_SectorObjectives = Column(NVARCHAR(500), primary_key=True, nullable=True)
    SectorAffected = Column(Integer, nullable=False)
    OtherSectors = Column(NVARCHAR(500), nullable=False)
    Objective = Column(Integer, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    Dimension = Column(Integer, nullable=False)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class OtherObjectives(Base):
    __tablename__ = "otherobjectives"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Other = Column(NVARCHAR(500), nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    Fk_SectorObjectives = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class UnionPolicyOther(Base):
    __tablename__ = "unionpolicyother"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    OtherUnionPolicy = Column(NVARCHAR(500), nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Entities(Base):
    __tablename__ = "entities"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Type = Column(Integer, nullable=False)
    Name = Column(NVARCHAR(500), nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Indicators(Base):
    __tablename__ = "indicators"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Description = Column(NVARCHAR(500), nullable=False)
    Unit = Column(NVARCHAR(500), nullable=False)
    Year1 = Column(Integer, nullable=False)
    Year2 = Column(Integer, nullable=False)
    Year3 = Column(Integer, nullable=False)
    Year4 = Column(Integer, nullable=False)
    Value1 = Column(NVARCHAR(500), nullable=False)
    Value2 = Column(NVARCHAR(500), nullable=False)
    Value3 = Column(NVARCHAR(500), nullable=False)
    Value4 = Column(NVARCHAR(500), nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Reference(Base):
    __tablename__ = "reference"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Reference = Column(NVARCHAR(500), nullable=False)
    URL = Column(Integer, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Table_2(Base):
    __tablename__ = "table_2"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    PolicyImpacting = Column(NVARCHAR(500), nullable=False)
    EUETS_1 = Column(Float, nullable=False)
    ESR_1 = Column(Float, nullable=False)
    LULUCF_1 = Column(Float, nullable=False)
    Total_1 = Column(Float, nullable=False)
    EUETS_2 = Column(Float, nullable=False)
    ESR_2 = Column(Float, nullable=False)
    LULUCF_2 = Column(Float, nullable=False)
    Total_2 = Column(Float, nullable=False)
    EUETS_3 = Column(Float, nullable=False)
    ESR_3 = Column(Float, nullable=False)
    LULUCF_3 = Column(Float, nullable=False)
    Total_3 = Column(Float, nullable=False)
    EUETS_4 = Column(Float, nullable=False)
    ESR_4 = Column(Float, nullable=False)
    LULUCF_4 = Column(Float, nullable=False)
    Total_4 = Column(Float, nullable=False)
    Explanation = Column(TEXT, nullable=False)
    FactorsAffected = Column(NVARCHAR(500), nullable=False)
    ExplanationMitigation = Column(TEXT, nullable=False)
    FactorAffectedPams = Column(NVARCHAR(500), nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Documentation_AnteAssessment(Base):
    __tablename__ = "documentation_anteassessment"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Reference = Column(NVARCHAR(500), nullable=False)
    WebLink = Column(Integer, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Documentation_PostAssessment(Base):
    __tablename__ = "documentation_postassessment"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Reference = Column(NVARCHAR(500), nullable=False)
    WebLink = Column(Integer, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class GHG_emissions(Base):
    __tablename__ = "ghg_emissions"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Year = Column(Integer, nullable=False)
    Value_EU_ETS = Column(Float, nullable=False)
    Value_ESR = Column(Float, nullable=False)
    Value_LULUCF = Column(Float, nullable=False)
    Value_Total = Column(Float, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class Table_3(Base):
    __tablename__ = "table_3"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    ProjectedCost = Column(Float, nullable=False)
    ProjectedAbsoluteCost = Column(Float, nullable=False)
    ProjectedBenefit = Column(Float, nullable=False)
    ProjectedAbsoluteBenefit = Column(Float, nullable=False)
    ProjectedNetCost = Column(Float, nullable=False)
    ProjectedAbsoluteNetCost = Column(Float, nullable=False)
    ProjectedYear = Column(Integer, nullable=False)
    ProjectedReferenceYear = Column(Integer, nullable=False)
    ProjectedDescriptionCost = Column(TEXT, nullable=False)
    ProjectedDescriptionNonGHG = Column(TEXT, nullable=False)
    RealizedCost = Column(Float, nullable=False)
    RealizedAbsoluteCost = Column(Float, nullable=False)
    RealizedBenefit = Column(Float, nullable=False)
    RealizedAbsoluteBenefit = Column(Float, nullable=False)
    RealizedNetCost = Column(Float, nullable=False)
    RealizedAbsoluteNetCost = Column(Float, nullable=False)
    RealizedYear = Column(Integer, nullable=False)
    RealizedReferenceYear = Column(Integer, nullable=False)
    RealizedDescriptionCost = Column(TEXT, nullable=False)
    RealizedDescriptionNonGHG = Column(TEXT, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    RealizedReferenceYearCurrency = Column(Integer, nullable=True)
    ProjectedReferenceYearCurrency = Column(Integer, nullable=True)
    RealizedReferenceYearCurrencyOther = Column(NVARCHAR(500), nullable=False)
    ProjectedReferenceYearCurrencyOther = Column(NVARCHAR(500), nullable=False)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class DocumentationCostEstimation_1(Base):
    __tablename__ = "documentationcostestimation_1"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Reference = Column(NVARCHAR(500), nullable=False)
    URL = Column(Integer, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


class DocumentationCostEstimation_2(Base):
    __tablename__ = "documentationcostestimation_2"
    __table_args__ = {"schema": "annex_XXIV"}

    Id = Column(Integer, primary_key=True)
    Reference = Column(NVARCHAR(500), nullable=False)
    URL = Column(Integer, nullable=False)
    Fk_PaMs = Column(Integer, nullable=True)
    ReportNet3HistoricReleaseId = Column(
        Integer, ForeignKey("ReportNet3HistoricReleases.Id")
    )
    ReportNet3HistoricReleases = relationship("ReportNet3HistoricReleases")


# %%


from sqlalchemy import create_engine

servername = "osprey"
dbname = "EnergyCommunityNationalSystemPAMS"
engine = create_engine(
    "mssql+pyodbc://@"
    + servername
    + "/"
    + dbname
    + "?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server"
)
conn = engine.connect()

# %%

from sqlalchemy.dialects import mssql
from sqlalchemy.schema import CreateTable

print(
    CreateTable(ReportNet3HistoricReleases.__table__).compile(dialect=mssql.dialect())
)


# %%  CREATE THE TABLES
from sqlalchemy import Table, Column, Integer, String, MetaData

rn3_hist = ReportNet3HistoricReleases()
rn3_hist.metadata.create_all(engine)
