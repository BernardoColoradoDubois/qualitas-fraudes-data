TEST_CONFIG = {
  "gce_cluster_config": {
    "internal_ip_only": True,
    "subnetwork_uri": "projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
    "service_account": "dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
    "shielded_instance_config": {
      "enable_secure_boot": False,
      "enable_vtpm": False,
      "enable_integrity_monitoring": False,
    }
  },
  "master_config": {
    "num_instances": 1,
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "worker_config": {
    "num_instances": 16,
     "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "secondary_worker_config": {
    "num_instances": 4,
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard",
      "boot_disk_size_gb": 32,
    },
    "is_preemptible": False,
  },
  "software_config": {
    "image_version":"2.1.85-debian11",
    "properties": {
      "dataproc:dataproc.conscrypt.provider.enable": "false",
      "capacity-scheduler:yarn.scheduler.capacity.resource-calculator":"org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
      "spark:spark.executor.cores": "1",                     # Reducir para tener más executors
      "spark:spark.executor.memory": "2g",                   # Ajustar memoria
      "spark:spark.driver.memory": "4g",                     # Mantener
      "spark:spark.executor.instances": "6",                 # Aumentar
      "spark:spark.yarn.am.memory": "1g",                   
      "spark:spark.dynamicAllocation.enabled": "true",      
      "spark:spark.dynamicAllocation.minExecutors": "5",    
      "spark:spark.dynamicAllocation.maxExecutors": "48",   
      "spark:spark.dynamicAllocation.initialExecutors": "30", 
      "spark:spark.scheduler.mode": "FAIR",                  
      "spark:spark.task.maxFailures": "8",
      "spark:spark.stage.maxConsecutiveAttempts": "4",
      "spark:spark.locality.wait": "10s",                   # Nuevo
      "spark:spark.shuffle.service.enabled": "true" ,         # Nuevo
      "spark:spark.executor.memoryOverhead": "512m",  # Ajustar overhead memory
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}


CLUSTER_CONFIG = {
  "gce_cluster_config": {
    "internal_ip_only": True,
    "subnetwork_uri": "projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
    "service_account": "dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
    "shielded_instance_config": {
      "enable_secure_boot": False,
      "enable_vtpm": False,
      "enable_integrity_monitoring": False,
    }
  },
  "master_config": {
    "num_instances": 1,
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "worker_config": {
    "num_instances": 12,
     "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "secondary_worker_config": {
    "num_instances": 4,
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard",
      "boot_disk_size_gb": 32,
    },
    "is_preemptible": False,
  },
  "software_config": {
    "image_version":"2.1.85-debian11",
    "properties": {
      "dataproc:dataproc.conscrypt.provider.enable": "false",
      "capacity-scheduler:yarn.scheduler.capacity.resource-calculator":"org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
      "spark:spark.executor.cores": "2",                     # Reducir de 10 a 2
      "spark:spark.executor.memory": "3g",                   # Añadir configuración de memoria
      "spark:spark.driver.memory": "4g",                     # Añadir memoria para el driver
      "spark:spark.executor.instances": "4",                 # Controlar número de executors
      "spark:spark.yarn.am.memory": "1g",                    # Memoria para YARN Application Master
      "spark:spark.dynamicAllocation.enabled": "true",       # Habilitar asignación dinámica
      "spark:spark.dynamicAllocation.minExecutors": "2",     # Mínimo de executors
      "spark:spark.dynamicAllocation.maxExecutors": "8",     # Máximo de executors
      "spark:spark.scheduler.mode": "FAIR"                   # Programador justo
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}


SMALL_CLUSTER_CONFIG = {
  "gce_cluster_config": {
    "internal_ip_only": True,
    "subnetwork_uri": "projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
    "service_account": "dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
    "shielded_instance_config": {
      "enable_secure_boot": False,
      "enable_vtpm": False,
      "enable_integrity_monitoring": False,
    }
  },
  "master_config": {
    "num_instances": 1,
    "machine_type_uri": "n2-highmem-8",  # Escalado: 8 vCPU, 64GB RAM
    "disk_config": {
      "boot_disk_type": "pd-ssd", 
      "boot_disk_size_gb": 100
    }
  },
  "worker_config": {
    "num_instances": 9,  # Escalado: de 4 a 9 workers
    "machine_type_uri": "n2-highmem-4",  # Mantiene: 4 vCPU, 32GB RAM per worker
    "disk_config": {
      "boot_disk_type": "pd-ssd", 
      "boot_disk_size_gb": 200
    }
  },
  "secondary_worker_config": {
    "num_instances": 0,  # Removed preemptible workers for stability
    "is_preemptible": False,
  },
  "software_config": {
    "image_version":"2.1.85-debian11",
    "properties": {
      # Core Spark configuration
      "spark:spark.scheduler.mode": "FAIR",
      "spark:spark.executor.memory": "6g",
      "spark:spark.driver.memory": "10g",  # Escalado: de 6g a 10g para manejar más pipelines
      "spark:spark.executor.cores": "2",
      "spark:spark.executor.memoryOverhead": "2g",
      
      # Dynamic allocation - Escalado para 20 pipelines
      "spark:spark.dynamicAllocation.enabled": "true",
      "spark:spark.dynamicAllocation.minExecutors": "4",  # Escalado: de 2 a 4
      "spark:spark.dynamicAllocation.maxExecutors": "18", # Escalado: de 8 a 18
      "spark:spark.dynamicAllocation.initialExecutors": "9", # Escalado: de 4 a 9
      
      # YARN configuration - Total cluster memory: 9 workers * 32GB = 288GB
      # Reserve ~4GB per node for system, so ~28GB available per worker
      "yarn:yarn.nodemanager.resource.memory-mb": "28672",  # 28GB per worker
      "yarn:yarn.scheduler.maximum-allocation-mb": "28672",
      "capacity-scheduler:yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
      
      # Spark SQL optimizations for Oracle JDBC
      "spark:spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
      "spark:spark.sql.sources.parallelPartitionDiscovery.threshold": "32",
      "spark:spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB
      "spark:spark.sql.sources.partitionOverwriteMode": "dynamic",
      "spark:spark.hadoop.parquet.memory.pool.ratio": "0.3",
      
      # Additional performance optimizations
      "spark:spark.yarn.am.memory": "4g",  # Escalado: de 2g a 4g
      "spark:spark.task.maxFailures": "8",
      "spark:spark.stage.maxConsecutiveAttempts": "4",
      "spark:spark.locality.wait": "10s",
      "spark:spark.shuffle.service.enabled": "true",
      "dataproc:dataproc.conscrypt.provider.enable": "false",
      
      # Adaptive query execution
      "spark:spark.sql.adaptive.enabled": "true",
      "spark:spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}


BIG_CLUSTER_CONFIG = {
  "gce_cluster_config": {
    "internal_ip_only": True,
    "subnetwork_uri": "projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
    "service_account": "dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
    "shielded_instance_config": {
      "enable_secure_boot": False,
      "enable_vtpm": False,
      "enable_integrity_monitoring": False,
    }
  },
  "master_config": {
    "num_instances": 1,
    "machine_type_uri": "n2-highmem-8",  # Escalado vertical 1.5x: 16 vCPU, 128GB RAM
    "disk_config": {
      "boot_disk_type": "pd-ssd", 
      "boot_disk_size_gb": 100  # Escalado 1.5x: 100GB → 150GB
    }
  },
  "worker_config": {
    "num_instances": 12,  # Escalado vertical 1.5x: 9 → 14 workers (redondeado)
    "machine_type_uri": "n2-highmem-4",  # Escalado vertical 1.5x: 8 vCPU, 64GB RAM per worker
    "disk_config": {
      "boot_disk_type": "pd-ssd", 
      "boot_disk_size_gb": 200  # Escalado 1.5x: 200GB → 300GB
    }
  },
  "secondary_worker_config": {
    "num_instances": 0,  # Removed preemptible workers for stability
    "is_preemptible": False,
  },
  "software_config": {
    "image_version":"2.1.85-debian11",
    "properties": {
      # Core Spark configuration
      "spark:spark.scheduler.mode": "FAIR",
      "spark:spark.executor.memory": "8g",  # Escalado vertical 1.5x: 6g → 9g
      "spark:spark.driver.memory": "13g",   # Escalado vertical 1.5x: 10g → 15g
      "spark:spark.executor.cores": "2",    # Escalado vertical 1.5x: 2 → 3 cores
      "spark:spark.executor.memoryOverhead": "3g",  # Escalado vertical 1.5x: 2g → 3g
      
      # Dynamic allocation - Escalado vertical 1.5x
      "spark:spark.dynamicAllocation.enabled": "true",
      "spark:spark.dynamicAllocation.minExecutors": "6",  # Escalado 1.5x: 4 → 6
      "spark:spark.dynamicAllocation.maxExecutors": "27", # Escalado 1.5x: 18 → 27
      "spark:spark.dynamicAllocation.initialExecutors": "10", # Escalado 1.5x: 9 → 14
      
      # YARN configuration - Total cluster memory: 14 workers * 64GB = 896GB
      # Reserve ~6GB per node for system, so ~58GB available per worker
      "yarn:yarn.nodemanager.resource.memory-mb": "35840",  # Escalado 1.5x: ~58GB per worker
      "yarn:yarn.scheduler.maximum-allocation-mb": "35840", # Escalado 1.5x
      "capacity-scheduler:yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
      
      # Spark SQL optimizations for Oracle JDBC
      "spark:spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
      "spark:spark.sql.sources.parallelPartitionDiscovery.threshold": "32",
      "spark:spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB
      "spark:spark.sql.sources.partitionOverwriteMode": "dynamic",
      "spark:spark.hadoop.parquet.memory.pool.ratio": "0.3",
      
      # Additional performance optimizations
      "spark:spark.yarn.am.memory": "5g",  # Escalado vertical 1.5x: 4g → 6g
      "spark:spark.task.maxFailures": "8",
      "spark:spark.stage.maxConsecutiveAttempts": "4",
      "spark:spark.locality.wait": "10s",
      "spark:spark.shuffle.service.enabled": "true",
      "dataproc:dataproc.conscrypt.provider.enable": "false",
      
      # Adaptive query execution
      "spark:spark.sql.adaptive.enabled": "true",
      "spark:spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}