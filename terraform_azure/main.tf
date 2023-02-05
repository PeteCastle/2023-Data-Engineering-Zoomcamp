terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 2.0"
    }
  }
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



