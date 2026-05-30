# terraform/environments/prod/main.tf

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  #backend "s3" {
   # bucket         = "tanilytics-terraform-state"
   # key            = "prod/terraform.tfstate"
   # region         = "eu-west-1"
   # dynamodb_table = "tanilytics-terraform-locks"
   # encrypt        = true
  #}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "tanilytics"
      Environment = "dev"
      ManagedBy   = "terraform"
    }
  }
}

module "vpc" {
  source = "../../modules/vpc"

  name = "tanilytics-dev"
  cidr = "10.0.0.0/16"

  azs = [
    "${var.aws_region}a",
    "${var.aws_region}b",
  ]

  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

module "eks" {
  source = "../../modules/eks"

  cluster_name    = "tanilytics-dev"
  cluster_version = var.eks_version
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.public_subnet_ids
  # Dev: one node group handles everything
  system_node_group = {
    instance_types = ["m7i-flex.large"]   
    capacity_type  = "ON_DEMAND"   
    min_size       = 3
    max_size       = 3
    desired_size   = 3
  }

  stateful_node_group = {
    instance_types = ["t3.xlarge"]
    capacity_type  = "ON_DEMAND"
    min_size       = 1
    max_size       = 2
    desired_size   = 1
    enabled        = false    # ← disabled
  }

  application_node_group = {
    instance_types = ["t3.large"]
    capacity_type  = "ON_DEMAND"
    min_size       = 1
    max_size       = 2
    desired_size   = 1
    enabled        = false    # ← disabled
  }

  processing_node_group = {
    instance_types = ["t3.large"]
    capacity_type  = "SPOT"
    min_size       = 1
    max_size       = 3
    desired_size   = 1
    enabled        = false    # ← disabled
  }
}
