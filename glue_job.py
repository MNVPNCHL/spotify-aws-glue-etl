import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node artist
artist_node1769654191086 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://pro-mnv/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1769654191086")

# Script generated for node tracks
tracks_node1769654192296 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://pro-mnv/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1769654192296")

# Script generated for node album
album_node1769654191767 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://pro-mnv/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1769654191767")

# Script generated for node Join album & artist
Joinalbumartist_node1769654254189 = Join.apply(frame1=artist_node1769654191086, frame2=album_node1769654191767, keys1=["id"], keys2=["artist_id"], transformation_ctx="Joinalbumartist_node1769654254189")

# Script generated for node Join with track
Joinwithtrack_node1769654294227 = Join.apply(frame1=tracks_node1769654192296, frame2=Joinalbumartist_node1769654254189, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtrack_node1769654294227")

# Script generated for node Drop Fields
DropFields_node1769654338246 = DropFields.apply(frame=Joinwithtrack_node1769654294227, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1769654338246")

# Script generated for node destination
EvaluateDataQuality().process_rows(frame=DropFields_node1769654338246, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769654186882", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
destination_node1769654357183 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1769654338246, connection_type="s3", format="glueparquet", connection_options={"path": "s3://pro-mnv/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="destination_node1769654357183")

job.commit()
