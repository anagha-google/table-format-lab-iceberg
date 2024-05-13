
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

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:"$BIGLAKE_CONN_SA" --role="roles/storage.objectViewer"
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
CREATE EXTERNAL TABLE loan_ds.biglake_iceberg_pit
  WITH CONNECTION `us-central1.loan-bl-conn`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://gcs-bucket-dll-hms-11002190840-e1664035-a2d9-4215-be46-45c40712/hive-warehouse/loan_db.db/loans_by_state_iceberg/metadata/00009-adcf98fc-d438-4d60-869b-058ed138dba6.metadata.json"]
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


## 3.1. Grant the UMSA BigLake admin privileges

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
LOCATION="us-central1"
UMSA="dll-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"


gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:"$UMSA" --role="roles/biglake.admin"

```

## 3.2. Create a Spark session with 
