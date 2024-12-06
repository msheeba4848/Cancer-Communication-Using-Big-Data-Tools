import sparknlp
import pyspark
from sparknlp.annotator import *
from sparknlp.pretrained import *
from pyspark.sql import SparkSession

print("Spark NLP version: ", sparknlp.version())

## You need to add the spark-nlp jar to the spark session

spark = SparkSession.builder \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \
    .getOrCreate()

print(f"Apache Spark version: {spark.version}")

spark.sparkContext.getConf().getAll()

import sys
print(f"Python version: {sys.version}")

from pip import _internal
print(f"pip version: {_internal.main(['show', 'pip'])}")

print("Pip packages:\n")
_internal.main(['list'])

print("Printing Spark COnfiguration")
print(spark.sparkContext.getConf().getAll())

print("Data Access Test")

blob_account_name = "dsan6000fall2024"
blob_container_name = "reddit-project"
wasbs_base_url = (
    f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/"
)
comments_path = "202306-202407/comments/"
submissions_path = "202306-202407/submissions/"

comments_df = spark.read.parquet(f"{wasbs_base_url}{comments_path}")
submissions_df = spark.read.parquet(f"{wasbs_base_url}{submissions_path}")

from pyspark.sql.functions import col, lower

# sample approximately 0.1 of the data
comments_subset_df = comments_df.sample(withReplacement=False, fraction=0.1, seed=42)

# Display the first few rows of subset DataFrame
comments_subset_df.show(5)

# Display the size (number of rows) in the df
print(f"Number of rows in the sampled and limited DataFrame: {comments_subset_df.count()}")

from pyspark.sql.functions import col, lower


# Filter for subreddits related to project
subreddit_list = ['CrohnsDisease', 'thyroidcancer', 'AskDocs', 'UlcerativeColitis', 'Autoimmune', 'BladderCancer', 'breastcancer', 'CancerFamilySupport', 'doihavebreastcancer', 'WomensHealth', 'ProstateCancer', 'cll', 'Microbiome', 'predental', 'endometrialcancer', 'cancer', 'Hashimotos', 'coloncancer', 'PreCervicalCancer', 'lymphoma', 'Lymphedema', 'CancerCaregivers', 'braincancer', 'lynchsyndrome', 'nursing', 'testicularcancer', 'leukemia', 'publichealth', 'Health', 'Fuckcancer', 'HealthInsurance', 'BRCA', 'Cancersurvivors', 'pancreaticcancer', 'skincancer', 'stomachcancer']

# Filter the DataFrame
filtered_comments_subset_df = comments_subset_df.filter(
    (col("subreddit").isin(subreddit_list))
)

# Filter the dataframe including body words of focus
government_healthcare_programs = [
    "medicare", "medicaid", "children’s health insurance program", "chip", 
    "veterans health administration", "vha", "indian health service", "ihs", 
    "federal employees health benefits program", "fehbp", "affordable care act", 
    "aca", "health insurance marketplace", "public health depart", 
    "local health depart", "national health service corps", "nhsc", 
    "community health centers", "chcs", "national institutes of health", "nih", 
    "nci", "national cancer institute"
]

cancer_charities = [
    "american cancer society", "acs", "cancer research institute", 
    "breast cancer research foundation", "bcrf", "leukemia lymphoma society", 
    "lls", "stand up to cancer", "su2c", "susan g. komen for the cure", 
    "st. jude children’s", "national foundation for cancer research", "nfcr", 
    "livestrong", "mesothelioma research foundation", "prostate cancer foundation", 
    "american brain tumor association", "abta", "colon cancer coalition", 
    "the american institute for cancer research", "aicr"
]

charitable_religious_organizations = [
    "catholic relief services", "crs", "world vision", "samaritan", 
    "jewish federations of north america", "islamic relief worldwide", 
    "buddhist global relief", "the salvation army", "christian aid", 
    "lutheran world relief", "tzu chi foundation", "care", 
    "cooperative for assistance and relief everywhere", "habitat for humanity", 
    "church world service", "cws", "heifer international"
]

top_cancer_institutes = [
    "researcher", "scientist", "physicians", "md anderson cancer center", 
    "memorial sloan kettering cancer center", "msk", "mayo clinic cancer center", 
    "johns hopkins sidney kimmel comprehensive cancer center", "cleveland clinic", 
    "ucla medical center", "massachusetts general hospital cancer center", 
    "duke cancer institute", "stanford cancer institute", 
    "university of california, san francisco medical center", "ucsf", 
    "northwestern medicine feinberg school of medicine", 
    "university of pennsylvania abramson cancer center", 
    "roswell park comprehensive cancer center", "fred hutchinson cancer research center"
]


filtered_comments_subset_df = filtered_comments_subset_df.filter(
    (((lower(col("body")).contains("frustrat"))) & 
     (lower(col("body")).contains("cancer"))) |
    ((lower(col("body")).contains("cancer")) & 
     ((lower(col("body")).contains("doctors")) | (lower(col("body")).contains("trust")))) |
    ((lower(col("body")).contains("cancer")) & 
     ((lower(col("body")).contains("family")) | (lower(col("body")).contains("friends")) |
      (lower(col("body")).contains("sister")) | (lower(col("body")).contains("brother")) |
      (lower(col("body")).contains("mother")) | (lower(col("body")).contains("mom")) |
      (lower(col("body")).contains("father")) | (lower(col("body")).contains("cousin")) |
      (lower(col("body")).contains("aunt")) | (lower(col("body")).contains("uncle")) |
      (lower(col("body")).contains("trust")))) |
    ((lower(col("body")).contains("cancer")) & 
     ((col("body").rlike("|".join(government_healthcare_programs))) | 
      (lower(col("body")).contains("trust")))) |
    ((lower(col("body")).contains("cancer")) & 
     ((col("body").rlike("|".join(cancer_charities))) | 
      (lower(col("body")).contains("trust")))) |
    ((lower(col("body")).contains("cancer")) & 
     ((col("body").rlike("|".join(charitable_religious_organizations))) | 
      (lower(col("body")).contains("trust")))) |
    ((lower(col("body")).contains("cancer")) & 
     ((col("body").rlike("|".join(top_cancer_institutes))) | 
      (lower(col("body")).contains("trust"))))
)


# Preview the filtered DataFrame
filtered_comments_subset_df.show(5)


workspace_default_storage_account = "projectgstoragedfb938a3e"
workspace_default_container = "azureml-blobstore-becc8696-e562-432e-af12-8a5e3e1f9b0f"
workspace_wasbs_base_url = f"wasbs://{workspace_default_container}@{workspace_default_storage_account}.blob.core.windows.net/"


# Save the filtered subset to a Parquet file
output_path = f"{workspace_wasbs_base_url}subset_job_0_2.parquet"
filtered_comments_subset_df.write.parquet(output_path, mode="overwrite")

print(f"Test results saved to {output_path}")

print("Data Access Test Passed")

