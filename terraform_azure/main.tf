terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 2.0"
    }
  }
}

locals{
  credentials = jsondecode(file("../credentials/credentials.json"))
}

provider "azurerm" {
  features{}
}

resource "azurerm_resource_group" "rg" {
  name     = "data-engineering-zoomcamp"
  location = "southeastasia"
}

resource "azurerm_storage_account" "sa" {
  name                     = "zoomcampsa"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier = "Hot"
}

resource "azurerm_storage_container" "sc" {
  name                  = "zoomcampcontainer"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "blob"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = "zoomcampfilesystem"
  storage_account_id = azurerm_storage_account.sa.id
}


resource "azurerm_synapse_workspace" "synapse"{
  name                                 = "zoomcampsynapse"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.fs.id
  sql_administrator_login = local.credentials["synapse_sql_admin"]
  sql_administrator_login_password = local.credentials["synapse_sql_password"]

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_synapse_firewall_rule" "synapse_firewall" {
  name                = "AllowAll"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}