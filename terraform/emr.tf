resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  release_label = var.emr_release_label
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }

  # Shut down after 15 minutes idle — avoids charges between daily runs
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  # Pre-warmed capacity cuts cold-start latency on the daily job
  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"
    initial_capacity_config {
      worker_count = 4
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "8 GB"
        disk   = "100 GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "40 vCPU"
    memory = "80 GB"
    disk   = "400 GB"
  }
}
