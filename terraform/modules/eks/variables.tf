variable "cluster_name" {
  type = string
}

variable "cluster_version" {
  type    = string
  default = "1.30"
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

# ── Node group configurations ─────────────────────────────────────────────────

variable "system_node_group" {
  description = "Configuration for the system node group (kube-system, ArgoCD, Kong, monitoring)"
  type = object({
    instance_types = list(string)
    capacity_type  = string
    min_size       = number
    max_size       = number
    desired_size   = number
  })
  default = {
    instance_types = ["t3.medium"]
    capacity_type  = "ON_DEMAND"
    min_size       = 2
    max_size       = 4
    desired_size   = 2
  }
}

variable "stateful_node_group" {
  description = "Configuration for stateful workloads (ClickHouse, Redpanda, PostgreSQL, Redis)"
  type = object({
    instance_types = list(string)
    capacity_type  = string
    min_size       = number
    max_size       = number
    desired_size   = number
    enabled        = bool
  })
  default = {
    instance_types = ["t3.xlarge"]
    capacity_type  = "ON_DEMAND"
    min_size       = 2
    max_size       = 4
    desired_size   = 2
    enabled        = true
  }
}

variable "application_node_group" {
  description = "Configuration for stateless application services"
  type = object({
    instance_types = list(string)
    capacity_type  = string
    min_size       = number
    max_size       = number
    desired_size   = number
    enabled        = bool
  })
  default = {
    instance_types = ["t3.large"]
    capacity_type  = "ON_DEMAND"
    min_size       = 2
    max_size       = 6
    desired_size   = 2
    enabled        = true
  }
}

variable "processing_node_group" {
  description = "Configuration for ingestion and processing services"
  type = object({
    instance_types = list(string)
    capacity_type  = string
    min_size       = number
    max_size       = number
    desired_size   = number
    enabled        = bool
  })
  default = {
    instance_types = ["t3.large", "t3.xlarge", "t3a.large"]
    capacity_type  = "SPOT"
    min_size       = 1
    max_size       = 10
    desired_size   = 2
    enabled        = true
  }
}