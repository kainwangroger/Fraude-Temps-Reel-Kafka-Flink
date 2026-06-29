terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  prefix = "${var.prefix}-${random_string.suffix.result}"
}

resource "azurerm_resource_group" "main" {
  name     = "${local.prefix}-rg"
  location = var.location
  tags     = var.tags
}

# Cosmos DB configuré avec l'API Cassandra
resource "azurerm_cosmosdb_account" "cassandra" {
  name                = "${local.prefix}-cosmos-cassandra"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  capabilities {
    name = "EnableCassandra"
  }

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = azurerm_resource_group.main.location
    failover_priority = 0
  }

  tags = var.tags
}

resource "azurerm_cosmosdb_cassandra_keyspace" "fraud" {
  name                = "fraud_keyspace"
  resource_group_name = azurerm_resource_group.main.name
  account_name        = azurerm_cosmosdb_account.cassandra.name
  throughput          = 400
}

# Storage Account pour le stockage persistant des configurations et data MinIO
resource "azurerm_storage_account" "storage" {
  name                     = replace("${local.prefix}store", "-", "")
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  tags                     = var.tags
}

resource "azurerm_storage_container" "minio_data" {
  name                  = "minio-data"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}

# Stack de conteneurs pour exécuter Kafka, Flink, Redis, Grafana et l'API
resource "azurerm_container_group" "streaming_stack" {
  name                = "${local.prefix}-cg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = local.prefix

  container {
    name   = "kafka"
    image  = "apache/kafka:3.9.0"
    cpu    = "1.0"
    memory = "2.0"

    environment_variables = {
      CLUSTER_ID                             = "fraud-cluster-001"
      KAFKA_NODE_ID                          = "1"
      KAFKA_PROCESS_ROLES                    = "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS         = "1@localhost:9093"
      KAFKA_LISTENERS                        = "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS             = "PLAINTEXT://kafka:9092"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "1"
    }

    ports {
      port     = 9092
      protocol = "TCP"
    }
  }

  container {
    name   = "redis"
    image  = "redis:alpine"
    cpu    = "0.5"
    memory = "1.0"

    ports {
      port     = 6379
      protocol = "TCP"
    }
  }

  container {
    name   = "metrics-exporter"
    image  = "metrics-exporter:latest" # Image construite en CI/CD
    cpu    = "0.5"
    memory = "1.0"

    environment_variables = {
      REDIS_HOST = "localhost"
    }

    ports {
      port     = 8001
      protocol = "TCP"
    }
  }

  container {
    name   = "grafana"
    image  = "grafana/grafana:latest"
    cpu    = "0.5"
    memory = "1.0"

    ports {
      port     = 3000
      protocol = "TCP"
    }
  }

  tags = var.tags
}
