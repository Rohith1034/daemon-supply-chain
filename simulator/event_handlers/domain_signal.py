DOMAIN_EVENT_TYPES = {
    "CustomerOrderCreated", "InventoryReserved", "PickingTaskCreated",
    "PickingStarted", "PickingCompleted", "PackingStarted", "Packed",
    "OutboundShipmentCreated", "OutForDelivery", "Delivered",
    "TruckLoaded", "TruckDeparted", "GPSUpdated", "DelayOccurred",
    "CheckpointReached", "ArrivedAtWarehouse", "ArrivedAtCustomer",
    "WorkerShiftStarted", "WorkerBreakStarted", "ForkliftFailure",
    "PutawayCompleted", "InventoryCycleCount", "InventoryAdjusted",
    "StockReserved", "StockReleased", "ReplenishmentTriggered",
    "ReorderCreated", "SafetyStockBreach", "DemandForecastUpdated",
    "PromotionStarted", "PromotionEnded", "SeasonalDemandSpike",
    "HeavyWeatherCondition",
}


def handle(context=None):
    return {"status": "SUCCESS", "created_events": []}