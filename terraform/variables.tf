locals {
    data_lake_bucket = "data_lake_bucket" 
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-east1"
  type = string
}

variable "zone" {
  description="Your project zone"
  default = "asia-east1-a"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}


variable "network" {
  description = "Network for your instance/cluster"
  default     = "default"
  type        = string
  
}

variable "image" {
  description = "Image for VM"
  default = "ubuntu-os-cloud/ubuntu-2004-lts"
  type = string
}