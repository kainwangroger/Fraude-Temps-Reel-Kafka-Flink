output "resource_group_name" {
  description = "Nom du groupe de ressources Azure"
  value       = azurerm_resource_group.main.name
}

output "cosmosdb_cassandra_endpoint" {
  description = "Point de terminaison Cassandra de Cosmos DB"
  value       = azurerm_cosmosdb_account.cassandra.endpoint
}

output "container_group_ip" {
  description = "IP publique du groupe de conteneurs de streaming"
  value       = azurerm_container_group.streaming_stack.ip_address
}

output "container_group_fqdn" {
  description = "FQDN du groupe de conteneurs de streaming"
  value       = azurerm_container_group.streaming_stack.fqdn
}
