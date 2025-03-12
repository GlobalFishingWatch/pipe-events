provider "google" {
  project = "gfw-int-infrastructure"
}
resource "google_cloudbuild_trigger" "gfw-tool-processing" {
  name     = "gfw-tool-processing-tag"
  location = "us-central1"


  github {
    name  = "gfw-tool"
    owner = "GlobalFishingWatch"
    push {
      tag          = ".*"
      invert_regex = false
    }

  }


  service_account = "projects/gfw-int-infrastructure/serviceAccounts/terraform-deployer@gfw-int-infrastructure.iam.gserviceaccount.com"
  build {

    step {
      id         = "docker build"
      name       = "gcr.io/cloud-builders/docker"
      entrypoint = "/bin/bash"
      args = [
        "-c",
        <<-EOF
         
          docker build \
            -t \
            us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/gfw-tool:$TAG_NAME \
            .
            
        EOF
      ]

    }

    images = ["us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/gfw-tool:$TAG_NAME"]



    options {
      logging = "CLOUD_LOGGING_ONLY"
    }
    timeout = "1200s"
  }
}
