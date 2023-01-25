variable "credentials" {
    description = "Link to json file"
    default = "D:/Educational Others/2023 Data Engineering Zoomcamp/credentials/alert-ability-351416-a704903a1d70.json"
}

locals {
    data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
    default = "alert-ability-351416"
    type = string
}
variable "region" {
    description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
    default = "asia-east2"
    type = string
}

variable "storage_class"{
    description = "Storage class type for your bucket. Check official docs for more info."
    default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}