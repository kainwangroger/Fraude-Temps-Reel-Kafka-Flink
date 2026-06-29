variable "location" {
  description = "La région Azure où déployer les ressources"
  type        = string
  default     = "East US"
}

variable "prefix" {
  description = "Préfixe utilisé pour nommer les ressources"
  type        = string
  default     = "fraud"
}

variable "cassandra_admin_user" {
  description = "Nom d'utilisateur administrateur pour Cassandra / Cosmos DB"
  type        = string
  default     = "cassandra_admin"
}

variable "cassandra_admin_password" {
  description = "Mot de passe administrateur pour Cassandra / Cosmos DB"
  type        = string
  sensitive   = true
  default     = "P@ssw0rd12345!"
}

variable "tags" {
  description = "Tags à appliquer à toutes les ressources"
  type        = map(string)
  default = {
    Environment = "Development"
    Project     = "Fraud-Detection-Flink"
    Owner       = "Data-Platform-Team"
  }
}
