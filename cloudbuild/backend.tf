terraform {
  backend "gcs" {
    bucket = "gfw-int-infrastructure-tfstate-us-central1"
    prefix = "cloudbuild-gfw-tool" # Not change for this project
  }
}
