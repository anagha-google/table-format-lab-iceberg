
# 1. Biglake (and Dataplex) related setup 

## 1.1. Enable APIs
```
gcloud services enable bigquery.googleapis.com 
gcloud services enable storage.googleapis.com
gcloud services enable bigqueryconnection.googleapis.com
gcloud services enable bigquerydatapolicy.googleapis.com
gcloud services enable bigquerystorage.googleapis.com
gcloud services enable biglake.googleapis.com
gcloud services enable dataplex.googleapis.com
gcloud services enable dataplex.googleapis.com
gcloud services enable datacatalog.googleapis.com
gcloud services enable datalineage.googleapis.com
```

## 1.2. Create a BigLake connection

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
PROJECT_NAME=`gcloud projects describe ${PROJECT_ID} | grep name | cut -d':' -f2 | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
LOCATION="us-central1"
BIGLAKE_CONNECTION_ID="loan-bl-conn"

RESPONSE=`bq mk --connection --location=$LOCATION --project_id=$PROJECT_ID --connection_type=CLOUD_RESOURCE $BIGLAKE_CONNECTION_ID`

```

## 1.3. Get BigLake connection SA

```
CONN_RES=`bq show --connection $PROJECT_ID.$LOCATION.$BIGLAKE_CONNECTION_ID`
BIGLAKE_CONN_SA=`bq show --connection $PROJECT_ID.$LOCATION.$BIGLAKE_CONNECTION_ID  | grep serviceAccountId | cut -d':' -f4 |tr -d '"' | tr -d '}' | xargs`
echo $BIGLAKE_CONN_SA
```

## 1.4. Grant the BigLake connection SA access to storage systems

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
UMSA="dll-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com" 

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:"$BIGLAKE_CONN_SA" --role="roles/storage.objectViewer"
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:"$BIGLAKE_CONN_SA" --role="roles/biglake.admin"
```

## 1.5. Grant yourself connection user 
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`

gcloud projects add-iam-policy-binding $PROJECT_ID --member=user:"$GCP_ACCOUNT_NAME" --role="roles/bigquery.connectionUser"
```


## 1.6. Create BigQuery dataset - loan_ds

In the BQ UI
```
CREATE SCHEMA loan_ds OPTIONS (location='us-central1');
```

<hr><hr>

# 2. BigLake Iceberg snapshot tables

## 2.1. About 
These are Biglake self-managed Iceberg tables for a point in time Iceberg snapshot.<br>
As the table data changes, the snapshot needs refreshing


## 2.2. Create table

Run this in the BQ UI:
```
CREATE OR REPLACE EXTERNAL TABLE loan_ds.loans_by_state_iceberg_biglake_snapshot
  WITH CONNECTION `us-central1.loan-bl-conn`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://gcs-bucket-dll-hms-11002190840-e1664035-a2d9-4215-be46-45c40712/hive-warehouse/loan_db.db/loans_by_state_iceberg/metadata/00009-ba57d4c5-06ce-4240-a335-5c1a524f420b.metadata.json"]
   )
```

## 2.3. Query the table in BigQuery UI
```
SELECT * FROM `delta-lake-diy-lab.loan_ds.biglake_iceberg_pit` where addr_state='IL'
```

Loan count: 17775

## 2.4. Update the data in the table via Spark

```
spark.sql("update loan_db.loans_by_state_iceberg set loan_count=9999 where addr_state='IL'")
```


## 2.5. Query the table in BigQuery UI
```
SELECT * FROM `delta-lake-diy-lab.loan_ds.biglake_iceberg_pit` where addr_state='IL'
```

You will get the error -`Not found: Files gs://gcs-bucket-dll-hms-11002190840-e1664035-a2d9-4215-be46-45c40712/hive-warehouse/loan_db.db/loans_by_state_iceberg/metadata/00009-adcf98fc-d438-4d60-869b-058ed138dba6.metadata.json`


## 2.6. Update the table definition to refect the latest manifest json


```
CREATE OR REPLACE EXTERNAL TABLE loan_ds.biglake_iceberg_pit
  WITH CONNECTION `us-central1.loan-bl-conn`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://gcs-bucket-dll-hms-11002190840-e1664035-a2d9-4215-be46-45c40712/hive-warehouse/loan_db.db/loans_by_state_iceberg/metadata/00015-d22b3c09-fe8b-433e-a596-ad5c8aa02088.metadata.json"]
   )
```

## 2.7. Query the table in BigQuery UI

```
SELECT * FROM `delta-lake-diy-lab.loan_ds.biglake_iceberg_pit` where addr_state='IL'
```

You should get the result 9999


<hr><hr>


# 3. BigLake Iceberg native unmanaged tables 

## 3.1. Create a GCS bucket for BLMT


## 3.2. Grant the UMSA BigLake admin privileges

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
LOCATION="us-central1"
UMSA="dll-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"


gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:"$UMSA" --role="roles/biglake.admin"

```

## 3.3. Create Dataproc interactive Spark with the BLMS jar

```
SERVERLESS_SPARK_RUNTIME=2.0

PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
SESSION_NAME="iceberg-lab"
LOCATION="us-central1"
HISTORY_SERVER_NAME="dll-sphs-${PROJECT_NBR}"
METASTORE_NAME="dll-hms-${PROJECT_NBR}"
DATA_WAREHOUSE_DIR="gs://iceberg-spark-bucket-${PROJECT_NBR}/iceberg-warehouse-dir"
SUBNET="projects/delta-lake-diy-lab/regions/us-central1/subnetworks/spark-snet"
UMSA="dll-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com" 
ICEBERG_PKG_COORDINATES=org.apache.iceberg:iceberg-spark-runtime-3.3_2.13:1.5.0
BLMS_JAR_GCS_URI=gs://spark-lib/biglake/biglake-catalog-iceberg1.5.0-0.1.1-with-dependencies.jar
HMS_URI="projects/$PROJECT_ID/locations/$LOCATION/services/${METASTORE_NAME}" 
BLMS_CATALOG="loans_iceberg_catalog"

gcloud beta dataproc sessions create spark $SESSION_NAME-$RANDOM  \
--project=${PROJECT_ID} \
--location=${LOCATION} \
--history-server-cluster="projects/$PROJECT_ID/regions/$LOCATION/clusters/${HISTORY_SERVER_NAME}" \
--property="spark.sql.catalog.loans_iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
--property="spark.sql.catalog.loans_iceberg_catalog.catalog-impl=org.apache.iceberg.gcp.biglake.BigLakeCatalog" \
--property="spark.sql.catalog.loans_iceberg_catalog.gcp_project=$PROJECT_ID" \
--property="spark.sql.catalog.loans_iceberg_catalog.gcp_location=$LOCATION" \
--property="spark.sql.catalog.loans_iceberg_catalog.blms_catalog=$BLMS_CATALOG" \
--property="spark.sql.catalog.loans_iceberg_catalog.warehouse=$DATA_WAREHOUSE_DIR" \
--property="spark.jars.packages=$ICEBERG_PKG_COORDINATES" \
--property="spark.jars=$BLMS_JAR_GCS_URI" \
--service-account=$UMSA \
--subnet=$SUBNET \
--version=$SERVERLESS_SPARK_RUNTIME
```

Run the BLMS lab notebook.

## 3.4. Edge cases - orphan table in BLMS without table listing in BQ


```
CREATE EXTERNAL TABLE `loan_ds.loans_by_state_iceberg`
    WITH CONNECTION `us-central1.loan-bl-conn` 
    OPTIONS(format='ICEBERG',   uris=['blms://projects/11002190840/locations/us-central1/catalogs/loans_iceberg_catalog/databases/loan_iceberg_db/tables/loans_by_state_iceberg']);
``
