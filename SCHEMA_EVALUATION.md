# Schema Evaluation Report: Supplier, Warehouse, Inventory & Inbound Events

**Date**: 2024
**Scope**: Supplier, Warehouse, Inventory, and Inbound Event schemas
**Status**: 🔴 **CRITICAL GAPS IDENTIFIED**

---

## Executive Summary

The current schema implementation has **functional coverage** but contains **multiple critical gaps and mismatches** that could impact:
- Data integrity and consistency
- Audit and compliance requirements
- Business process completeness
- Performance and scalability

---

## 1. CRITICAL ISSUES

### 1.1 Event Outbox Schema - Missing Resilience Fields

**Issue**: The `event_outbox` table lacks retry and delivery tracking mechanisms.

**Current Schema**:
```sql
CREATE TABLE event_outbox (
    id bigint PRIMARY KEY,
    event_id uuid NOT NULL,
    event_type varchar(100) NOT NULL,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id varchar(100) NOT NULL,
    correlation_id varchar(100),
    payload jsonb NOT NULL,
    created_at timestamp DEFAULT now(),
    published_at timestamp,
    status varchar(30) DEFAULT 'PENDING',
    PRIMARY KEY (id)
);
```

**Gaps**:
- ❌ No `retry_count` field - cannot track how many times event delivery was attempted
- ❌ No `error_message` field - cannot capture why delivery failed
- ❌ No `failed_at` field - cannot track when last delivery attempt failed
- ❌ No `scheduled_retry_at` field - cannot implement exponential backoff
- ❌ No `sequence_number` - cannot guarantee event ordering
- ❌ `correlation_id` nullable but may need to be NOT NULL

**Impact**: 
- Cannot implement proper retry logic for failed event deliveries
- No visibility into what caused delivery failures
- Risk of orphaned events that never reach consumers
- Cannot guarantee event ordering for critical business processes

**Recommendation**:
```sql
ALTER TABLE event_outbox ADD COLUMN (
    retry_count integer DEFAULT 0,
    max_retries integer DEFAULT 5,
    error_message text,
    failed_at timestamp with time zone,
    scheduled_retry_at timestamp with time zone,
    sequence_number bigint UNIQUE,
    last_attempted_at timestamp with time zone
);

ALTER TABLE event_outbox ALTER COLUMN correlation_id SET NOT NULL;
```

---

### 1.2 Shipments Table - Missing Inbound Tracking Details

**Issue**: The `shipments` table is missing critical inbound logistics information.

**Current Schema**:
```sql
CREATE TABLE shipments (
    shipment_id varchar(30) PRIMARY KEY,
    po_id varchar(30),
    supplier_id varchar(20),
    warehouse_id varchar(20),
    shipment_status varchar(30),
    shipment_date timestamp,
    expected_delivery timestamp,
    actual_delivery timestamp,
    total_skus integer,
    total_quantity integer,
    created_at timestamp,
    updated_at timestamp
);
```

**Gaps**:
- ❌ No `tracking_number` / `awb_number` - cannot track shipment in transit
- ❌ No `carrier_id` / `carrier_name` - cannot identify logistics provider
- ❌ No `shipping_mode` (ground, air, sea) - cannot optimize logistics
- ❌ No `receiving_dock_id` - cannot assign receiving location
- ❌ No `grn_status` (Goods Receipt Notification) - cannot track receiving confirmation
- ❌ No `quality_check_status` - cannot track quality inspections
- ❌ No `insurance_amount` / `insurance_provider` - cannot track risk coverage
- ❌ No `weight_kg` / `dimensions` - cannot optimize warehouse space
- ❌ No `receiving_completed_at` field - cannot track receiving process timing

**Impact**:
- Cannot match inbound shipments with carrier tracking
- No visibility into receiving process (is it putaway or pending QC?)
- Cannot track quality issues at receipt
- Missing weight/dimension data for receiving dock planning
- No audit trail for warehouse receiving operations

**Recommendation**:
```sql
ALTER TABLE shipments ADD COLUMN (
    tracking_number varchar(100),
    carrier_id varchar(20),
    carrier_name varchar(255),
    shipping_mode varchar(50), -- ground, air, sea, rail
    receiving_dock_id varchar(30),
    grn_number varchar(100),
    grn_received_at timestamp with time zone,
    quality_check_status varchar(30), -- PENDING, PASSED, FAILED, PARTIAL
    quality_check_notes text,
    quality_checked_at timestamp with time zone,
    quality_checked_by varchar(30),
    insurance_amount numeric,
    insurance_provider varchar(255),
    weight_kg numeric,
    length_cm numeric,
    width_cm numeric,
    height_cm numeric,
    receiving_completed_at timestamp with time zone,
    receiving_completed_by varchar(30)
);

-- Add foreign key for receiving dock and warehouse staff
ALTER TABLE shipments 
ADD CONSTRAINT fk_shipments_dock 
FOREIGN KEY (receiving_dock_id) REFERENCES warehouse_locations(location_id);
```

---

### 1.3 Shipment Items - No Audit Trail for Receipt Discrepancies

**Issue**: The `shipment_items` table lacks detailed tracking of receipt vs. expected discrepancies.

**Current Schema**:
```sql
CREATE TABLE shipment_items (
    shipment_item_id bigint PRIMARY KEY,
    shipment_id varchar(30),
    product_id varchar(30),
    shipped_quantity integer,
    received_quantity integer DEFAULT 0,
    damaged_quantity integer DEFAULT 0
);
```

**Gaps**:
- ❌ No `expected_quantity` field - cannot identify discrepancies
- ❌ No `shortfall_quantity` field - cannot track missing items
- ❌ No `excess_quantity` field - cannot track over-deliveries
- ❌ No reason code for discrepancies (e.g., CARRIER_DAMAGE, SUPPLIER_ERROR, etc.)
- ❌ No `quality_grade` field - cannot track quality levels (A, B, C)
- ❌ No `received_at` timestamp - cannot track when each item was received
- ❌ No `received_by` field - cannot audit who received the items
- ❌ No `putaway_status` - cannot track if items were stored after receipt

**Impact**:
- Cannot easily identify what was shipped vs. what was received
- No accountability for damage or losses
- Cannot distinguish between supplier errors and carrier damage
- Missing quality tracking at item level

**Recommendation**:
```sql
ALTER TABLE shipment_items ADD COLUMN (
    expected_quantity integer,
    shortfall_quantity integer,
    excess_quantity integer,
    discrepancy_reason varchar(100), -- CARRIER_DAMAGE, SUPPLIER_ERROR, MISSING, EXCESS
    quality_grade varchar(10), -- A, B, C, REJECT
    received_at timestamp with time zone,
    received_by varchar(30),
    putaway_status varchar(30), -- PENDING, IN_PROGRESS, COMPLETED
    putaway_completed_at timestamp with time zone,
    putaway_location_id varchar(30)
);

ALTER TABLE shipment_items
ADD CONSTRAINT fk_items_location
FOREIGN KEY (putaway_location_id) REFERENCES warehouse_locations(location_id);
```

---

### 1.4 Inventory Table - Missing Lot Tracking and Expiration

**Issue**: The `inventory` table doesn't support lot tracking or expiration date management (critical for perishables).

**Current Schema**:
```sql
CREATE TABLE inventory (
    inventory_id bigint PRIMARY KEY,
    product_id varchar(30),
    warehouse_id varchar(20),
    on_hand_quantity integer DEFAULT 0,
    reserved_quantity integer DEFAULT 0,
    damaged_quantity integer DEFAULT 0,
    available_quantity integer,
    safety_stock integer DEFAULT 0,
    reorder_point integer DEFAULT 0,
    reorder_quantity integer DEFAULT 0,
    last_updated_at timestamp,
    location_id varchar(30)
);
```

**Gaps**:
- ❌ No `lot_number` / `batch_number` - cannot track product lots
- ❌ No `expiration_date` - cannot manage perishables
- ❌ No `manufacture_date` - cannot track product age
- ❌ No `first_in_first_out_quantity` (FIFO tracking) - inventory method required
- ❌ No `warehouse_zone_quantity` breakdown - cannot optimize picking
- ❌ No `bin_quantity` - cannot track bin-level quantities
- ❌ No `unit_of_measure` - assuming all are in same UOM
- ❌ Available quantity calculation is missing triggers

**Impact**:
- Cannot manage expiration dates (regulatory requirement for food/pharma)
- Cannot implement FIFO inventory management
- Cannot track which lot was damaged or missing
- Cannot optimize picking efficiency by zone

**Recommendation**:
```sql
-- Create new lot tracking table
CREATE TABLE inventory_lots (
    lot_id bigint PRIMARY KEY DEFAULT nextval('inventory_lots_id_seq'),
    inventory_id bigint NOT NULL,
    lot_number varchar(100) NOT NULL,
    batch_number varchar(100),
    manufacture_date date,
    expiration_date date,
    received_date date,
    quantity_received integer,
    quantity_available integer,
    quantity_reserved integer,
    quantity_damaged integer,
    fifo_sequence integer,
    created_at timestamp DEFAULT now(),
    FOREIGN KEY (inventory_id) REFERENCES inventory(inventory_id)
);

ALTER TABLE inventory ADD COLUMN (
    unit_of_measure varchar(20), -- EA, KG, L, etc.
    lot_tracking_enabled boolean DEFAULT false
);

-- Add trigger to calculate available_quantity
CREATE OR REPLACE FUNCTION update_available_quantity()
RETURNS TRIGGER AS $$
BEGIN
    NEW.available_quantity := NEW.on_hand_quantity - NEW.reserved_quantity - NEW.damaged_quantity;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER inventory_update_available
BEFORE INSERT OR UPDATE ON inventory
FOR EACH ROW
EXECUTE FUNCTION update_available_quantity();
```

---

### 1.5 Suppliers Table - Missing Performance Tracking

**Issue**: The `suppliers` table lacks critical supplier performance and compliance data.

**Current Schema**:
```sql
CREATE TABLE suppliers (
    supplier_id varchar(20) PRIMARY KEY,
    supplier_name varchar(255),
    supplier_type varchar(50),
    category_supported text,
    contact_email varchar(255),
    contact_phone varchar(50),
    country varchar(100),
    state varchar(100),
    city varchar(100),
    rating numeric,
    payment_terms varchar(20),
    lead_time_days integer,
    status varchar(20),
    created_at timestamp,
    updated_at timestamp
);
```

**Gaps**:
- ❌ No `payment_term_days` / `payment_term_description` - varchar(20) too restrictive
- ❌ No `on_time_delivery_percentage` - cannot track KPI
- ❌ No `quality_score` - cannot measure quality performance
- ❌ No `order_fill_rate` - cannot measure completeness
- ❌ No `defect_rate` - cannot track supplier quality issues
- ❌ No `shipping_cost_average` - cannot negotiate better rates
- ❌ No `supplier_account_manager` - cannot track POC
- ❌ No `bank_account_details` / `tax_id` - missing payment/compliance info
- ❌ No `contract_start_date` / `contract_end_date` - missing contract lifecycle
- ❌ No `minimum_order_quantity` - cannot prevent small orders
- ❌ No `packaging_type` - cannot verify packaging compliance
- ❌ No `last_audit_date` - missing compliance tracking

**Impact**:
- Cannot make data-driven supplier selection decisions
- Missing payment and tax compliance information
- Cannot track supplier contract validity
- No visibility into supplier performance metrics

**Recommendation**:
```sql
ALTER TABLE suppliers ADD COLUMN (
    payment_term_days integer,
    payment_term_description varchar(255),
    on_time_delivery_percentage numeric,
    quality_score numeric,
    order_fill_rate numeric,
    defect_rate numeric,
    average_shipping_cost numeric,
    supplier_account_manager varchar(255),
    account_manager_email varchar(255),
    tax_id varchar(50),
    bank_account_number varchar(100),
    bank_code varchar(20),
    bank_name varchar(255),
    contract_start_date date,
    contract_end_date date,
    minimum_order_quantity integer,
    minimum_order_value numeric,
    packaging_type varchar(100),
    certification_types text, -- JSON array or CSV
    last_audit_date date,
    audit_result varchar(50), -- PASS, FAIL, CONDITIONAL
    performance_rating integer, -- 1-5 stars
    risk_level varchar(20), -- LOW, MEDIUM, HIGH
    blocked boolean DEFAULT false,
    block_reason text,
    blocked_since timestamp
);
```

---

### 1.6 Warehouses Table - Missing Operational Capacity Tracking

**Issue**: The `warehouses` table has overall capacity but missing per-zone capacity tracking.

**Current Schema** (partial):
```sql
CREATE TABLE warehouses (
    warehouse_id varchar(20) PRIMARY KEY,
    storage_capacity_units bigint,
    -- ... other fields
    temperature_controlled boolean,
    temperature_min_c numeric,
    temperature_max_c numeric,
    -- ... etc
);
```

**Gaps**:
- ❌ No zone-level capacity tracking (storage_capacity per zone type)
- ❌ No `current_occupancy_units` - cannot calculate utilization %
- ❌ No `cost_per_unit_per_day` - cannot calculate storage costs
- ❌ No `peak_throughput_units_per_day` - cannot plan receiving schedules
- ❌ No `minimum_temperature_setpoint` / `maximum_temperature_setpoint` - redundant field names
- ❌ No warehouse manager contact information
- ❌ No warehouse opening/closing hours breakdown per day
- ❌ No equipment list (WMS version, conveyor types, etc.)

**Impact**:
- Cannot calculate warehouse utilization in real-time
- Cannot track cost per storage
- Cannot forecast receiving capacity issues
- Missing warehouse management contacts

**Recommendation**:
```sql
ALTER TABLE warehouses ADD COLUMN (
    current_occupancy_units bigint,
    occupancy_percentage numeric,
    cost_per_unit_per_day numeric,
    peak_throughput_units_per_day integer,
    warehouse_manager_name varchar(255),
    warehouse_manager_email varchar(255),
    warehouse_manager_phone varchar(50),
    monday_hours varchar(50),
    tuesday_hours varchar(50),
    wednesday_hours varchar(50),
    thursday_hours varchar(50),
    friday_hours varchar(50),
    saturday_hours varchar(50),
    sunday_hours varchar(50),
    wms_vendor varchar(100),
    wms_version varchar(50),
    last_updated_timestamp_at timestamp with time zone
);

-- Create zone capacity tracking table
CREATE TABLE warehouse_zone_capacity (
    zone_capacity_id bigint PRIMARY KEY DEFAULT nextval('warehouse_zone_capacity_id_seq'),
    warehouse_id varchar(20),
    zone varchar(50),
    storage_type varchar(50),
    total_capacity_units bigint,
    current_occupancy_units bigint,
    reserved_units bigint,
    available_units bigint,
    created_at timestamp DEFAULT now(),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);
```

---

### 1.7 Warehouse Locations Table - Missing Utilization Tracking

**Issue**: The `warehouse_locations` table doesn't track location-level utilization.

**Current Schema**:
```sql
CREATE TABLE warehouse_locations (
    location_id varchar(30) PRIMARY KEY,
    warehouse_id varchar(20),
    zone varchar(50),
    aisle varchar(20),
    rack varchar(20),
    shelf varchar(20),
    bin varchar(20),
    storage_type varchar(50),
    capacity_units integer,
    current_utilization integer DEFAULT 0,
    temperature_controlled boolean,
    hazmat_allowed boolean,
    status varchar(20) DEFAULT 'ACTIVE',
    created_at timestamp
);
```

**Gaps**:
- ❌ No `utilization_percentage` calculation
- ❌ No `last_stocked_at` - cannot track staleness
- ❌ No `rotation_type` (FIFO, LIFO, Random) - cannot enforce inventory method
- ❌ No `blocked_reason` - if status is BLOCKED, need to know why
- ❌ No `blocked_since` - when was it blocked
- ❌ No weight limit per location
- ❌ No incompatibility matrix (e.g., hazmat + fragile)
- ❌ No list of currently stored products

**Impact**:
- Cannot optimize location assignment
- Cannot enforce inventory rotation rules
- Missing weight/structural constraints
- Cannot plan putaway efficiency

**Recommendation**:
```sql
ALTER TABLE warehouse_locations ADD COLUMN (
    utilization_percentage numeric,
    last_stocked_at timestamp with time zone,
    rotation_type varchar(20), -- FIFO, LIFO, RANDOM
    blocked_reason varchar(255),
    blocked_since timestamp with time zone,
    max_weight_kg numeric,
    current_weight_kg numeric,
    compatible_storage_types varchar(500), -- JSON array
    incompatible_items_list text
);
```

---

## 2. MEDIUM PRIORITY ISSUES

### 2.1 Inventory Allocations & Reservations - No Audit Trail

**Issue**: Missing fields to track allocation and reservation lifecycle.

**Current**:
```sql
CREATE TABLE inventory_allocations (
    allocation_id varchar(30) PRIMARY KEY,
    order_id varchar(30),
    warehouse_id varchar(20),
    product_id varchar(30),
    allocated_quantity integer,
    allocation_status varchar(30),
    allocated_at timestamp
);
```

**Missing**:
- ❌ `allocated_by` - user who allocated
- ❌ `released_at` - when was allocation released
- ❌ `released_by` - who released it
- ❌ `allocation_location_id` - where was it allocated from
- ❌ `actual_picked_quantity` - may differ from allocated

**Recommendation**:
```sql
ALTER TABLE inventory_allocations ADD COLUMN (
    allocated_by varchar(30),
    released_at timestamp with time zone,
    released_by varchar(30),
    allocation_location_id varchar(30),
    actual_picked_quantity integer,
    FOREIGN KEY (allocation_location_id) REFERENCES warehouse_locations(location_id)
);
```

---

### 2.2 Purchase Orders - Missing Approval & Audit Data

**Issue**: The `purchase_orders` table lacks approval workflow and audit information.

**Gaps**:
- ❌ No `approved_by` field
- ❌ No `approved_at` field
- ❌ No `created_by` field
- ❌ No `po_revision_number` - no versioning
- ❌ No `receiving_location_id` - where should PO be received
- ❌ No `payment_due_date` - when must we pay
- ❌ No `freight_cost` - not included in line items
- ❌ No `tax_amount` - not tracked
- ❌ No `discount_amount` - not tracked
- ❌ No `internal_notes` - for internal discussions

**Recommendation**:
```sql
ALTER TABLE purchase_orders ADD COLUMN (
    created_by varchar(30),
    approved_by varchar(30),
    approved_at timestamp with time zone,
    po_revision_number integer DEFAULT 1,
    receiving_location_id varchar(30),
    payment_due_date date,
    freight_cost numeric,
    freight_carrier varchar(255),
    tax_amount numeric,
    discount_amount numeric,
    internal_notes text,
    FOREIGN KEY (receiving_location_id) REFERENCES warehouse_locations(location_id)
);
```

---

### 2.3 Missing Data Audit Fields Across Tables

**Issue**: No `created_by` / `updated_by` / `deleted_at` fields for audit compliance.

**Impact**: Cannot track who made changes or audit delete operations.

**Tables Missing Audit Info**:
- `suppliers` - no `created_by`, `updated_by`, `deleted_at`
- `warehouses` - no `created_by`, `updated_by`, `deleted_at`
- `warehouse_locations` - no `created_by`, `updated_by`, `deleted_at`
- `inventory` - no `deleted_at` (soft delete)
- `products` - no `deleted_at` (soft delete)

**Recommendation**:
```sql
-- Add to all relevant tables:
ALTER TABLE suppliers ADD COLUMN (
    created_by varchar(30),
    updated_by varchar(30),
    deleted_at timestamp with time zone
);

-- Similar for warehouses, warehouse_locations, inventory, products
```

---

### 2.4 Workers Table - No Productivity Tracking

**Issue**: The `workers` table is defined but `worker_service.py` doesn't exist.

**Gaps**:
- ❌ `worker_productivity` table exists but no service to update it
- ❌ No `last_active_at` field to track worker availability
- ❌ No `total_tasks_completed` running counter
- ❌ No `average_task_time` - computed field
- ❌ No `shift_schedule` - detailed schedule

**Recommendation**:
- Create `WorkerService` to manage worker data
- Add triggers to update `worker_productivity` on task completion

---

## 3. DATA TYPE & CONSISTENCY ISSUES

### 3.1 Inconsistent Timestamp Types

**Issue**: Timestamps use different types across schema:

```
✅ CORRECT (with time zone):
- event_outbox.created_at
- shipments.shipment_date
- orders.order_date

❌ INCORRECT (without time zone):
- warehouse_tasks.created_at
- worker_productivity.recorded_at
- warehouse_locations.created_at
- workers.created_at
- inventory_locations.created_at
```

**Recommendation**: Standardize to `timestamp with time zone` for all timestamps.

---

### 3.2 Inconsistent ID Formats

**Issue**: Different patterns for ID generation:

```
SHIP-20240101120000         (ShipmentService)
PO-XXXXX                    (POService - inconsistent pattern)
ORD-XXXXX                   (Orders)
CUST-XXXXX                  (Customers)
```

**Recommendation**: Document and enforce consistent ID generation patterns.

---

## 4. MISSING TRANSACTIONAL CONSTRAINTS

### 4.1 Cascading Delete Issues

**Issue**: No CASCADE DELETE policies defined. What happens when:
- A supplier is deleted? (Orphaned POs, shipments, products)
- A warehouse is deleted? (Orphaned inventory, locations)
- A location is deleted? (Orphaned inventory)

**Recommendation**:
```sql
ALTER TABLE purchase_orders DROP CONSTRAINT fk_supplier;
ALTER TABLE purchase_orders ADD CONSTRAINT fk_purchase_orders_supplier
  FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id) 
  ON DELETE RESTRICT
  ON UPDATE CASCADE;

ALTER TABLE products DROP CONSTRAINT fk_supplier;
ALTER TABLE products ADD CONSTRAINT fk_products_supplier
  FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
  ON DELETE RESTRICT; -- prevent deletion if products exist
```

---

### 4.2 Missing Check Constraints

**Issue**: No business logic constraints:

```
- inventory.on_hand_quantity >= 0
- inventory.reserved_quantity >= 0
- warehouse.occupancy_percentage BETWEEN 0 AND 100
- supplier.rating BETWEEN 0 AND 5
- workers.experience_years >= 0
```

**Recommendation**:
```sql
ALTER TABLE inventory ADD CONSTRAINT chk_on_hand_qty_positive
  CHECK (on_hand_quantity >= 0);

ALTER TABLE inventory ADD CONSTRAINT chk_reserved_qty_positive
  CHECK (reserved_quantity >= 0);

ALTER TABLE warehouse_tasks ADD CONSTRAINT chk_qty_positive
  CHECK (quantity > 0);
```

---

## 5. PERFORMANCE ISSUES

### 5.1 Missing Indexes

**Critical Indexes Missing**:

```sql
-- For inventory queries
CREATE INDEX idx_inventory_product_warehouse 
  ON inventory(product_id, warehouse_id);

CREATE INDEX idx_inventory_warehouse 
  ON inventory(warehouse_id);

-- For shipment queries
CREATE INDEX idx_shipments_po_id 
  ON shipments(po_id);

CREATE INDEX idx_shipments_status 
  ON shipments(shipment_status);

CREATE INDEX idx_shipments_warehouse 
  ON shipments(warehouse_id);

-- For event queries
CREATE INDEX idx_event_outbox_status 
  ON event_outbox(status, created_at);

CREATE INDEX idx_event_outbox_aggregate 
  ON event_outbox(aggregate_type, aggregate_id);

-- For allocation/reservation queries
CREATE INDEX idx_inventory_allocations_order 
  ON inventory_allocations(order_id);

CREATE INDEX idx_inventory_reservations_order 
  ON inventory_reservations(order_id);
```

---

## 6. CRITICAL ACTION ITEMS

### Immediate (P0) - Must Fix Before Production:

1. ✅ **Event Outbox Resilience** - Add retry tracking
2. ✅ **Shipment Inbound Details** - Add tracking, carrier, QC info
3. ✅ **Inventory Lot Tracking** - Add lot/batch/expiration support
4. ✅ **Data Type Consistency** - Standardize timestamp types
5. ✅ **Cascade Delete Rules** - Define clear referential integrity

### Short-term (P1) - Next Sprint:

6. Supplier performance metrics
7. Warehouse utilization tracking
8. Audit fields (created_by, updated_by, deleted_at)
9. Location utilization and constraints
10. Performance indexes

### Medium-term (P2) - Backlog:

11. Payment and tax compliance fields
12. Contract lifecycle management
13. Zone and bin level tracking
14. Worker productivity integration
15. Quality inspection workflow

---

## 7. IMPLEMENTATION PRIORITY MATRIX

| Issue | Impact | Effort | Priority |
|-------|--------|--------|----------|
| Event Outbox Resilience | HIGH | MEDIUM | **P0** |
| Shipment Inbound Details | HIGH | MEDIUM | **P0** |
| Inventory Lot Tracking | MEDIUM | HIGH | **P0** |
| Timestamp Standardization | LOW | LOW | **P0** |
| Cascade Deletes | MEDIUM | LOW | **P0** |
| Supplier Metrics | MEDIUM | MEDIUM | P1 |
| Audit Fields | MEDIUM | MEDIUM | P1 |
| Warehouse Capacity | MEDIUM | MEDIUM | P1 |
| Performance Indexes | LOW | MEDIUM | P1 |
| Payment/Compliance | MEDIUM | HIGH | P2 |

---

## 8. RISK ASSESSMENT

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Lost/Orphaned Events | **CRITICAL** | Implement retry logic + monitoring |
| Shipment Discrepancies Untracked | **CRITICAL** | Add QC + discrepancy fields |
| Expired Inventory Issues | **HIGH** | Add lot tracking + expiration logic |
| Data Integrity Violations | **HIGH** | Add constraints + cascade rules |
| Audit/Compliance Gap | **HIGH** | Add audit fields + soft deletes |
| Performance Degradation | **MEDIUM** | Add indexes + optimize queries |

---

## Conclusion

The current schema has a solid foundation but requires **multiple critical additions** before production use, particularly around:
1. Event resilience and retry logic
2. Inbound receiving process tracking
3. Lot and expiration date management
4. Audit and compliance fields
5. Data integrity constraints

**Estimated effort**: 3-4 weeks for P0 items + schema migration testing.
