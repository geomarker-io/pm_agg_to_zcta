# pm_agg_to_zcta

This code is used to aggregate daily Brokamp h3 PM2.5 predictions to 2010 ZCTAs via a crosswalk generated using area-weighted averaging.  For more details on the procedure, see https://github.com/geomarker-io/zcta_to_h3.

`make_zcta_pm.R` creates daily zipcode estimates and uploads them to S3 as `s3://pm25-brokamp/zcta_2010/pm_01XXX.rds`, where the `01` denotes that this file contains all estimates for five digits ZCTAs that start with the first two digits of `01`.

This means zipcode data can be accessed by inserting its first two digits into the S3 URI. Each file contains a `zcta` column, a `date` column, and a column containing the predicted PM2.5 called `wt_pm_pred`.


