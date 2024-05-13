
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

## 2. Create a BigLake connection

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
PROJECT_NAME=`gcloud projects describe ${PROJECT_ID} | grep name | cut -d':' -f2 | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
LOCATION="us-central1"
BIGLAKE_CONNECTION_ID="loan-bl-conn"

RESPONSE=`bq mk --connection --location=$LOCATION --project_id=$PROJECT_ID --connection_type=CLOUD_RESOURCE $BIGLAKE_CONNECTION_ID`

```

## 3. Get BigLake connection SA

```
CONN_RES=`bq show --connection $PROJECT_ID.$LOCATION.$BIGLAKE_CONNECTION_ID`
BIGLAKE_CONN_SA=`bq show --connection $PROJECT_ID.$LOCATION.$BIGLAKE_CONNECTION_ID  | grep serviceAccountId | cut -d':' -f4 |tr -d '"' | tr -d '}' | xargs`
echo $BIGLAKE_CONN_SA
```

## 4. Grant the BigLake connection SA access to storage systems

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:"$BIGLAKE_CONN_SA" --role="roles/storage.objectViewer"
```

## 5. Grant yourself connection user 
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`

gcloud projects add-iam-policy-binding $PROJECT_ID --member=user:"$GCP_ACCOUNT_NAME" --role="roles/bigquery.connectionUser"
```


## 6. Create BigQuery dataset - loan_ds

In the BQ UI
```
CREATE SCHEMA loan_ds;
```

<hr><hr>

# 2. BigLake Iceberg snapshot tables

These are Biglake self-managed Iceberg tables for an Iceberg snapshot.

