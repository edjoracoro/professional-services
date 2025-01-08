# Billboard Overview
This code implements GCS Insight dataset and create a Looker Studio Dashboard.



## Environment set-up

You can set-up the right python environment as follows:
```
cd examples/gcs_insight
rm -rf bill-env
python3 -m venv bill-env
source bill-env/bin/activate
pip install -r requirements.txt
```
This step includes the following:
- Install Python local env
- Launch local env
- Install dependencies

## To see options
```
python gcs_insight_2.0.py -h
```
## Required Flags
 -project project ID that contains the BQ datasets
 -dataset The dataset name of the GCS Insight views
```

python gcs_insight_2.0.py -project 'my-project'  -dataset 'gcs_insight' 

```



```