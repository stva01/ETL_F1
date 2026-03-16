
# these are the terraform variables they store certain values which we dont want to hardcode in our terraform scrips 



variable "aws_region" {
  default = "ap-south-1"
}

variable "project_name" {
  default = "f1-pipeline"
}

variable "mode" {
  description = "paused or active — controls what gets spun up"
  default     = "paused"
}
