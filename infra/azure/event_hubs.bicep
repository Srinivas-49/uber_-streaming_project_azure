// Optional always-billed Standard Event Hubs namespace.
// NEVER deploy as part of GitHub CI. Inspect regional pricing first.
targetScope = 'resourceGroup'
@minLength(3)
@maxLength(12)
param prefix string = 'portuber01'
param location string = resourceGroup().location

var nameSuffix = uniqueString(resourceGroup().id)
resource namespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: concat('eh-', prefix, '-', nameSuffix)
  location: location
  sku: { name: 'Standard', tier: 'Standard', capacity: 1 }
  properties: {
    isAutoInflateEnabled: false
    publicNetworkAccess: 'Enabled'
    minimumTlsVersion: '1.2'
  }
}
resource hub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  name: 'uber-ride-events'
  parent: namespace
  properties: { partitionCount: 2, messageRetentionInDays: 1 }
}
output namespaceName string = namespace.name
output hubName string = hub.name
