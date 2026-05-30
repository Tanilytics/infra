# terraform/modules/eks/node-groups.tf

# ── Shared IAM role for all node groups ───────────────────────────────────────

resource "aws_iam_role" "node" {
  name = "${var.cluster_name}-node"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_worker" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# ── System node group ─────────────────────────────────────────────────────────
# Always enabled — runs kube-system, ArgoCD, Kong, monitoring

resource "aws_eks_node_group" "system" {
  cluster_name    = module.eks.cluster_name
  node_group_name = "system"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.system_node_group.instance_types
  capacity_type  = var.system_node_group.capacity_type

  scaling_config {
    min_size     = var.system_node_group.min_size
    max_size     = var.system_node_group.max_size
    desired_size = var.system_node_group.desired_size
  }

  labels = {
    role = "system"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}

# ── Stateful node group ───────────────────────────────────────────────────────
# Runs: ClickHouse, Redpanda, PostgreSQL, Redis
# In dev: disabled — everything runs on system nodes

resource "aws_eks_node_group" "stateful" {
  count = var.stateful_node_group.enabled ? 1 : 0

  cluster_name    = module.eks.cluster_name
  node_group_name = "stateful"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.stateful_node_group.instance_types
  capacity_type  = var.stateful_node_group.capacity_type

  scaling_config {
    min_size     = var.stateful_node_group.min_size
    max_size     = var.stateful_node_group.max_size
    desired_size = var.stateful_node_group.desired_size
  }

  labels = {
    role = "stateful"
  }

  taint {
    key    = "workload"
    value  = "stateful"
    effect = "NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}

# ── Application node group ────────────────────────────────────────────────────
# Runs: Auth, Query, Privacy, Dashboard
# In dev: disabled — everything runs on system nodes

resource "aws_eks_node_group" "application" {
  count = var.application_node_group.enabled ? 1 : 0

  cluster_name    = module.eks.cluster_name
  node_group_name = "application"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.application_node_group.instance_types
  capacity_type  = var.application_node_group.capacity_type

  scaling_config {
    min_size     = var.application_node_group.min_size
    max_size     = var.application_node_group.max_size
    desired_size = var.application_node_group.desired_size
  }

  labels = {
    role = "application"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}

# ── Processing node group ─────────────────────────────────────────────────────
# Runs: Ingestion, Processing services
# In dev: disabled — everything runs on system nodes

resource "aws_eks_node_group" "processing" {
  count = var.processing_node_group.enabled ? 1 : 0

  cluster_name    = module.eks.cluster_name
  node_group_name = "processing"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.processing_node_group.instance_types
  capacity_type  = var.processing_node_group.capacity_type

  scaling_config {
    min_size     = var.processing_node_group.min_size
    max_size     = var.processing_node_group.max_size
    desired_size = var.processing_node_group.desired_size
  }

  labels = {
    role = "processing"
  }

  taint {
    key    = "workload"
    value  = "processing"
    effect = "NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}