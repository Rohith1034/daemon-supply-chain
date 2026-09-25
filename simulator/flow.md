I’m reviewing the project as it exists today and will ground the assessment in the actual implementation files, entry points, and validation outputs rather than proposing changes to the design.

Searched for regex `FastAPI|Flask|if __name__ == ['\"]__main__['\"]|run_simulation_cycle|class EventQueue|class EventExecutor|event_outbox|Outbox` (`**/*.py`), 143 results

Read 

Read , lines 1 to 260

Read , lines 1 to 260

Read 

Read , lines 260 to 330

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read , lines 1 to 260

Read , lines 260 to 420

Read , lines 1 to 220

Read , lines 220 to 420

Read 

Read , lines 1 to 240

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read 

Read 

## Executive Architecture Review

This project is a Python-based supply-chain event simulator with a clear event-driven execution model already implemented in-process. The runtime path is not a generic message bus yet; it is a scheduler-driven simulation loop backed by PostgreSQL and a durable event-outbox table.

The current architecture already contains the core production-style pieces:
- generator functions
- database writes
- outbox persistence
- future-event scheduling
- scheduler dispatch
- executor retry logic
- state validation
- event chaining

What is clearly not implemented yet:
- Kafka producer/consumer
- background polling worker loop
- dedicated event-bus infrastructure
- service-to-service orchestration

The strongest evidence is in:
- `master_simulator.py`
- `event_queue.py`
- `event_executor.py`
- `outbox.py`
- `event_state_machine.py`
- `__init__.py`

---

## 1. Project execution flow

### 1.1 Entry point and top-level runtime

The main runtime loop is in `master_simulator.py`.

Relevant function:
- run_simulation_cycle

Flow:
1. `master_simulator.py` creates or receives an EventQueue.
2. It creates EventScheduler with the queue and default registry.
3. It reads the last checkpoint from SimulationCheckpoint.
4. It calls queue.list_ready_events(current_time, from_time=last_checkpoint).
5. It iterates through every due event.
6. For each event, scheduler.executor.execute(...) runs.
7. If the handler returns created_events, it calls _schedule_returned_events and inserts those future rows back into the queue.
8. It advances the checkpoint to the simulation time.
9. It returns a result object containing processed events, newly scheduled events, and queue status.

This is the actual orchestration loop.

### 1.2 Flow by stage

Simulator
↓
Generator
↓
Database
↓
Outbox
↓
Scheduler
↓
Queue
↓
Executor
↓
Handler
↓
State Machine
↓
Future Event Creation
↓
Queue Again

#### Stage A — Simulator
File: `master_simulator.py`

Function:
- run_simulation_cycle

What it does:
- Builds queue, scheduler, checkpoint
- Resolves current simulation time
- Retrieves due future events
- Executes due events
- Inserts newly scheduled child events

What it returns:
- dict with:
  - simulation_time
  - last_checkpoint
  - processed_events
  - scheduled_events
  - ready_events
  - queue_size

Where it goes next:
- Into the scheduler and executor path

#### Stage B — Generator
Example file: `purchase_order_created.py`

Function:
- generate_purchase_order

What it does:
- Selects active supplier and warehouse
- Resolves products and quantities
- Writes purchase_orders and purchase_order_items rows
- Constructs payload
- Calls publish_event

What it returns:
- a generated payload and event publication result
- typically, business logic does not directly return a scheduler event object

Where it goes next:
- PostgreSQL writes, then outbox insert

#### Stage C — Database
Files:
- `db.py`
- `database_schema.json`

Function:
- Database.__enter__ / __exit__
- Database.execute / fetch_one / fetch_all

What it does:
- Opens PostgreSQL connection
- Runs SQL inserts/updates/selects
- Commits on success, rolls back on exception

What it returns:
- rows, generated IDs, status of operations

Where it goes next:
- event_outbox insert via publish_event

#### Stage D — Outbox
File: `outbox.py`

Function:
- publish_event

What it does:
- Inserts a row into event_outbox
- Stores:
  - event_id
  - event_type
  - aggregate_type
  - aggregate_id
  - correlation_id
  - payload
  - created_at
  - status

What it returns:
- event_id

Where it goes next:
- EventQueue reads this row as a future event

#### Stage E — Scheduler
File: `scheduler.py`

Function:
- EventScheduler.dispatch_ready_events
- EventScheduler.run_batch

What it does:
- Calls queue.list_ready_events(...)
- Loops through due rows
- Invokes executor.execute(event, registry=self.registry)

What it returns:
- processed results list

Where it goes next:
- executor

#### Stage F — Queue
File: `event_queue.py`

Function:
- EventQueue.list_ready_events
- EventQueue.insert_future_event
- EventQueue.mark_event_status

What it does:
- Reads due future events
- Orders by priority and scheduled time
- Allows retry-scheduled events to be re-eligible when next_retry_time is due
- Marks status transitions

What it returns:
- list of ready rows/events

Where it goes next:
- executor

#### Stage G — Executor
File: `event_executor.py`

Function:
- EventExecutor.execute

What it does:
- Resolves event handler
- Validates state transition
- Starts event execution
- Calls handler
- Handles exceptions and retry backoff
- Persists execution metadata
- Schedules default next event when configured

What it returns:
- result dict with status and created_events or retry info

Where it goes next:
- result is inspected in master_simulator and also fed to future event scheduling

#### Stage H — Handler
Files:
- `__init__.py`
- `base.py`

Function:
- build_handler
- handle(event_context)

What it does:
- Resolves generator function
- Calls generator
- Normalizes created_events list
- Preserves aggregate_type, aggregate_id, correlation_id
- Returns a contract dictionary

What it returns:
- {"status": "SUCCESS", "created_events": [...], "result": {...}}

Where it goes next:
- master_simulator._schedule_returned_events converts those created_events into future queue rows

#### Stage I — State Machine
File: `event_state_machine.py`

Function:
- validate_transition
- infer_next_state
- infer_entity_type

What it does:
- Checks whether current state can move to next state
- Maps event names to entity and state
- Rejects invalid transitions

What it returns:
- boolean or inferred state/entity mapping

Where it goes next:
- if valid, execution continues; if invalid, executor fails the event

#### Stage J — Future Event Creation
File: `event_executor.py`

Functions:
- _build_default_next_event
- _schedule_returned_events in `master_simulator.py`

What it does:
- Reads the NEXT_EVENT_SEQUENCE map
- Builds the next future event
- Uses get_future_event_time to set a future timestamp
- Carries correlation_id and aggregate metadata forward

What it returns:
- next event spec dict

Where it goes next:
- queue.insert_future_event(...)

#### Stage K — Queue Again
File: `event_queue.py`

Function:
- insert_future_event

What it does:
- Persists new event row with due timestamp
- Priority is stored and used in ordering
- Can be in-memory or PostgreSQL-backed

What it returns:
- event_id

Where it goes next:
- next iteration of scheduler when current_time reaches the scheduled timestamp

---

## 2. Project folder structure

| Folder | Purpose | Responsibility | Important files | How other modules use it |
|---|---|---|---|---|
| `simulator` | main simulation runtime | contains simulation engine, generators, handlers, and core runtime classes | `master_simulator.py`, `core`, `generators`, `event_handlers` | central runtime package imported by Airflow, tests, and validation |
| `core` | execution infrastructure | clock, db, queue, executor, outbox, state machine, checkpoint, timing | `db.py`, `outbox.py`, `checkpointing.py`, `event_state_machine.py` | all scheduler/executor logic depends on it |
| `generators` | domain event creation | inserts business data and writes outbox rows | `purchase_order_created.py`, `order_created.py`, `goods_received.py` | generators are called through handler factory and event registry |
| `event_handlers` | adapter layer between scheduler and generators | binds generator functions to event names and normalizes created_events | `__init__.py`, `base.py`, `purchase_order_created.py` | scheduler resolves handler by event type |
| `loading_scripts` | legacy or manual operational scripts | run flows, load reference data | `run_event_flow.py`, `load_products.py` | older entry points for direct simulation execution |
| `api` | external API layer | exposes simulation endpoints | `main.py`, `simulator_routes.py`, `simulator_service.py` | external clients invoke simulation via HTTP |
| `airflow` | orchestration trigger | schedules simulator runs | `simulation_trigger_dag.py` | triggers simulation as a DAG task |
| `tests` | regressions and validation | verifies scheduler semantics, event order, idempotency, API contract | `test_event_scheduler.py`, `test_master_simulator.py`, `validator_utils.py` | used as CI and validation harness |
| `output` | runtime artifacts | checkpoint files, validation reports, generated JSON artifacts | `validation_report.json`, `event_execution_report.json` | generated by simulator and validation output |
| `scripts` | utility scripts | validation and operational runner | `run_simulator_validation.py` | user-run validation entry point |
| `postgres` and `migrations` | schema and data setup | database migration and bootstrap objects | not shown as direct runtime components in the execution path | database setup support, not the live event loop |
| `kafka` | placeholder or future integration area | not active in run path | there are kafka-related directories, but I did not find Kafka producers/consumers in the execution logic | present as future design space only |

### Important note
I did not find a Kafka production path in the active runtime. The live flow goes through PostgreSQL and the in-process scheduler. This is not a speculation; it is based on the actual code path used by `master_simulator.py`, `event_scheduler`, and `outbox.py`.

---

## 3. Entry points

### 3.1 Runtime entry points

| Entry point | File | Starts | Purpose |
|---|---|---|---|
| Simulator runtime | `master_simulator.py` | when run as script or imported | executes due queued future events |
| Airflow trigger | `simulation_trigger_dag.py` | Airflow DAG | schedules simulation batches |
| FastAPI app | `main.py` | uvicorn or similar | exposes HTTP endpoints |
| Validation runner | `run_simulator_validation.py` | script execution | generates validation report |
| Legacy manual flow scripts | `run_event_flow.py` and sibling scripts | script execution | older direct run paths |

### 3.2 Actual execution behavior

#### Master simulator
File: `master_simulator.py`

The main logic is:
- run_simulation_cycle(...)
- __main__ block prints the returned cycle result

This is the core event-processing loop for the simulator.

#### Airflow
File: `simulation_trigger_dag.py`

This defines a DAG with:
- dag_id = simulation_trigger_dag
- schedule_interval = "0 */6 * * *"
- PythonOperator calling run_simulation_cycle

This means the scheduler can be driven by an orchestration trigger, but it is not a self-running background service in the code.

#### API
Files:
- `main.py`
- `simulator_routes.py`
- `simulator_service.py`

Endpoints found:
- GET /health
- POST /simulate/full-flow
- GET /simulate/report

This API is a thin wrapper around simulation scripts and report retrieval. It is not the core event loop itself.

#### Validation
Files:
- `test_event_scheduler.py`
- `validator_utils.py`
- `run_simulator_validation.py`

This is a test and validation harness. It is not the runtime event path.

---

## 4. Event lifecycle

### 4.1 PurchaseOrderCreated lifecycle

#### 1) Generator
File: `purchase_order_created.py`

Function:
- generate_purchase_order

What happens:
- picks supplier and warehouse
- finds products
- creates purchase_orders row
- creates purchase_order_items rows
- builds payload
- calls publish_event

Database writes:
- purchase_orders
- purchase_order_items

Outbox:
- `outbox.py` insert into event_outbox

Scheduler and queue:
- `event_queue.py`
- EventQueue.list_ready_events determines due events by scheduled_time
- EventQueue.insert_future_event persists the future event or reads from event_outbox

Executor:
- `event_executor.py`
- EventExecutor.execute calls the handler

Handler:
- `purchase_order_created.py`
- build_handler for purchase_order_created.generate_purchase_order

State machine:
- `event_state_machine.py`
- validates PurchaseOrder: CREATED -> APPROVED

Next event:
- EventExecutor.NEXT_EVENT_SEQUENCE defines:
  - PurchaseOrderCreated -> PurchaseOrderApproved

This is the transition map in `event_executor.py`.

Result:
- It creates the next future event and enqueues it.

#### 2) PurchaseOrderApproved lifecycle
This follows the same pattern:
- generator function in `purchase_order_approved.py`
- event registry in `__init__.py`
- EventExecutor.NEXT_EVENT_SEQUENCE continues to SupplierShipmentCreated

### 4.2 OrderCreated lifecycle

File: `order_created.py`

Execution path:
1. choose customer and warehouse
2. create orders row
3. build payload with order details
4. publish_event to outbox
5. queue selects it based on scheduled_time
6. EventExecutor.execute resolves handler
7. EventHandler registry maps event to generator factory
8. state machine validates Order: CREATED -> ALLOCATED / RESERVED
9. next event is OrderItemCreated
10. EventQueue inserts next event
11. chain continues through inventory allocation, reservation, picking, packing, shipment creation

This shows the same pattern as PurchaseOrderCreated but for outbound flow.

### 4.3 Event chain pattern
The project is designed around chained future events, not ad hoc handler invocation.

Evidence:
- `event_executor.py`
- NEXT_EVENT_SEQUENCE is a declarative chain map of:
  - event -> next event type
  - aggregate type
  - entity type
  - current state
  - next state
  - priority

This is the production model for event choreography.

---

## 5. Event generators

### 5.1 How generators work

Generators are business-data functions under `generators`.

Examples:
- `purchase_order_created.py`
- `order_created.py`
- `goods_received.py`

Pattern:
- connect to DB
- select domain entities
- write business rows
- build payload
- call publish_event
- log success

### 5.2 How they select records

Examples:
- Purchase order generator selects active supplier and warehouse by selectors in `selectors.py`
- Order generator selects a random customer and a warehouse with available inventory
- inventory or warehouse tasks select by current actual table state

This is not a random event simulation without context; it reads database state first.

### 5.3 How they update the DB

They use Database.execute with plain SQL inserts.

Examples:
- purchase_orders insert in `purchase_order_created.py`
- orders insert in `order_created.py`
- warehouse tasks and shipments follow the same pattern across the generator tree

### 5.4 How payloads are created

Most payloads are created using business-specific helper functions or inline dicts.

Examples:
- `payloads.py`
- `purchase_order_created.py` calls build_purchase_order_created_payload
- `order_created.py` builds a dict payload in-line

The payload is the event contract that goes into event_outbox.

### 5.5 How events are published

The publication point is `outbox.py`.

publish_event inserts into event_outbox with:
- event_id as uuid
- event_type
- aggregate_type
- aggregate_id
- correlation_id
- payload JSONB

This is the event publication mechanism currently used by the simulator.

### 5.6 How correlation IDs propagate

The pattern is consistent:
- generator creates local correlation_id value
- writes it to business tables when relevant
- stores it in event_outbox payload
- scheduler/executor/context passes it along
- handler normalization copies it to child events when missing

Evidence:
- `base.py`
- _coerce_correlation_id
- _normalize_created_event

This is a strong implementation detail: each created future event inherits correlation_id unless overwritten.

### 5.7 How aggregate IDs propagate

Used in `base.py` and `event_executor.py`.

Methods:
- _coerce_aggregate_id
- _build_default_next_event

It prefers:
- aggregate_id_field passed by handler
- aggregate_id
- po_id
- shipment_id
- order_id
- task_id
- inventory_id

This means child future events preserve aggregate identity unless a different aggregate legitimately changes.

Assumption:
The aggregate propagation is designed to be stable and domain-correct, but the exact long-term correctness across every event depends on the generator and its schema contract. I did not find a single global domain model beyond the state machine and event sequence map.

---

## 6. Outbox pattern

### 6.1 How it works

The outbox is implemented as PostgreSQL table event_outbox in `database_schema.json`.

The insert is done in `outbox.py` by publish_event.

The read path is in `event_queue.py`:
- list_ready_events executes SELECT * FROM event_outbox WHERE status IN ('PENDING', 'RETRY_SCHEDULED') AND payload->>'event_scheduled_time' <= target_time

The marking path is in the same file:
- mark_event_status executes UPDATE event_outbox SET status = %s, published_at = COALESCE(published_at, %s) WHERE event_id = %s

### 6.2 Who inserts rows
- generators via publish_event
- in same Database transaction as the business record writes

This is the key transactional outbox pattern idea.

### 6.3 Who reads rows
- EventQueue.list_ready_events in `event_queue.py`

### 6.4 Who marks processed
- EventQueue.mark_event_status in `event_queue.py`

### 6.5 Whether it follows a standard transactional outbox pattern

Short answer: yes, in a limited but real sense.

Why:
- the event record is written as part of the same database transaction used by the generator
- the event is stored in a dedicated outbox table
- a queue/scheduler reads the outbox table later
- status changes mark processing

What it does not do yet:
- no Kafka producer
- no external relay process
- no separate outbox worker service
- no consumer-based decoupling

So this is a database-backed transactional outbox implementation, but not yet an event-bus integration layer.

---

## 7. Scheduler

### 7.1 How scheduler works

Main file: `scheduler.py`

EventScheduler:
- holds queue and executor
- registry is defaulted to EVENT_REGISTRY
- dispatch_ready_events -> calls queue.list_ready_events
- executes each due event

### 7.2 Scheduling logic

Scheduling is defined by:
- `event_timing.py`
- `simulation_clock.py`

Key behavior:
- get_simulation_now reads environment variable SIMULATION_NOW or real current UTC
- get_future_event_time(event_type, base_time) adds a random delay based on EVENT_DELAY_WINDOWS

This means scheduling logic is deterministic relative to the simulation clock, not wall-clock logic from the handler.

### 7.3 Delayed events

Each future event contains:
- scheduled_time
- payload.event_scheduled_time
- priority

Queue checks whether scheduled_time <= target_time.

### 7.4 Retries

Retry handling is in `event_executor.py`:
- exception path
- retry_count increments
- max_retry_count defaults to 3
- next_retry_time = now + computed interval
- status becomes RETRY_SCHEDULED

Backoff:
- _compute_retry_interval returns exponential backoff in minutes:
  - min(5 * (2 ** (retry_count - 1)), 60)

### 7.5 Execution timing

EventExecutor adds latency:
- EVENT_PROCESSING_LATENCY_SECONDS = 1 second
- started_at = created_at + 1 second

This is not a real worker heartbeat; it is just a modeled processing latency.

### 7.6 Polling interval

I did not find a continuous polling loop or background thread.

The scheduler is invoked on demand:
- Airflow DAG triggers run_simulation_cycle
- master_simulator itself loops through ready events
- API calls a script that runs the flow

This is a “batch-triggered scheduler,” not a continuously running daemon.

### 7.7 Checkpointing

File: `checkpointing.py`

Behavior:
- last processed simulation time is persisted to JSON
- advance_to(value) writes it
- read() reads it back
- default path is `last_simulation_checkpoint.json`

This is how the system resumes from the last processed simulation moment.

### 7.8 Recovery

The queue and checkpoint work together:
- queue.list_ready_events accepts from_time
- master_simulator passes last checkpoint time to restore processing from the gap

This is a simple recovery mechanism rather than a full distributed consistency model.

---

## 8. Queue

### 8.1 Queue implementation

File: `event_queue.py`

The queue supports:
- memory_mode
- Postgres-backed persistence

Memory mode:
- holds _memory_events as list
- stores execution_history
- state can be saved to event_queue_state.json

DB-backed mode:
- reads and writes directly to event_outbox table
- gets ready rows using SQL select
- updates status using SQL update

### 8.2 Priority

Priority ordering:
- PRIORITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

Used in list_ready_events sorting.

### 8.3 Ordering

Orders by:
- priority rank
- scheduled_time

This is a strict ready queue ordering model.

### 8.4 Scheduled events / future events

The future-event contract is:
- event_id
- event_type
- aggregate_type
- aggregate_id
- correlation_id
- payload
- scheduled_time
- priority

The queue itself does not do complex event routing; it simply selects due events and passes them to the executor.

### 8.5 Persistence

Persisted rows are in:
- event_outbox in `database_schema.json`

For memory mode, persisted state goes to:
- output/event_queue_state.json

### 8.6 How duplicates are prevented

There are two relevant checks:
1. In memory mode, EventExecutor._already_completed checks the in-memory execution log.
2. In the scheduler path, the event_row is processed as due only once per cycle in typical usage.

I did not find a DB-level unique constraint enforcement on event_id or deduplication query before selecting ready events. The duplicate prevention is mostly execution-level and test-based.

Assumption:
The system assumes each runtime invocation receives due events only once, and duplicate suppression is enforced by the execution log and the scheduler’s cycle semantics rather than a formal outbox de-duper.

---

## 9. Event executor

### 9.1 Responsibilities

File: `event_executor.py`

The executor is the core processing unit. It is responsible for:
- looking up event handler
- validating state transition
- measuring execution start
- invoking the handler
- handling exceptions
- applying retry policy
- scheduling child events
- updating event status/history

### 9.2 Handler lookup

- registry passed into execute(event_row, registry=...)
- default resolves to EVENT_REGISTRY from `event_registry.py`

### 9.3 Error handling

It has:
- InvalidStateTransition
- retry logic on exceptions
- FAILED status after max retries
- queue.mark_event_status(...)

### 9.4 Retries

Configured in:
- _compute_retry_interval
- retry_count
- max_retry_count
- next_retry_time

This is a built-in retry model, not a full saga or compensation mechanism.

### 9.5 Status updates

Statuses used:
- PENDING
- RETRY_SCHEDULED
- RUNNING
- COMPLETED
- FAILED
- SKIPPED

The queue records these state transitions.

### 9.6 History

EventQueue.execution_history collects status history in memory mode.
The event history is appended in mark_event_status when status in {"COMPLETED", "FAILED", "SKIPPED"}.

### 9.7 Logging

The project uses print-based logging in `logger.py`, not Python logging module. There is no central logger config or rotating file handler in the active runtime.

---

## 10. Handlers

### 10.1 Registration

Files:
- `__init__.py`
- `base.py`

Registration is via an EVENT_HANDLER_REGISTRY dict.

Examples:
- PurchaseOrderCreated -> `purchase_order_created.py`
- PurchaseOrderApproved -> `purchase_order_approved.py`
- SupplierShipmentCreated -> `supplier_shipment_created.py`

### 10.2 Handler factory

The factory is:
- build_handler(generator_ref, event_type, aggregate_type, aggregate_id_field)

It:
- resolves the generator function
- invokes it
- reads created_events from returned payload
- normalizes each created event
- adds missing correlation_id, aggregate_type, aggregate_id if absent

### 10.3 Normalization

Handled in `base.py`:
- _normalize_created_event
- _coerce_correlation_id
- _coerce_aggregate_id

This layer ensures child events conform to a consistent contract.

### 10.4 created_events

The contract is:
- dict with:
  - event_type
  - aggregate_type
  - aggregate_id
  - correlation_id
  - scheduled_time
  - payload

This is then converted into queue rows by master_simulator._schedule_returned_events.

### 10.5 Scheduling child events

This happens in `master_simulator.py` via:
- _schedule_returned_events

It normalizes each created event and calls:
- queue.insert_future_event(...)

### 10.6 Payload validation

Validation is primarily at runtime by:
- EventExecutor._validate_state_transition
- `event_state_machine.py`

It checks:
- payload.current_state
- payload.next_state
- inferred entity type

### 10.7 Metadata normalization

The handler layer creates a stable event metadata structure:
- aggregate_type
- aggregate_id
- correlation_id
- scheduled_time

This is the event contract the scheduler uses.

---

## 11. State machine

### 11.1 File
`event_state_machine.py`

### 11.2 State model

The project defines state machines for:
- PurchaseOrder
- Shipment
- Inventory
- WarehouseTask
- Order

### 11.3 Example transitions

#### PurchaseOrder
- CREATED -> APPROVED
- CREATED -> CANCELLED
- APPROVED -> CANCELLED

#### Shipment
- CREATED -> ASN_RECEIVED
- CREATED -> READY
- READY -> ASSIGNED
- ASSIGNED -> LOADED
- LOADED -> IN_TRANSIT
- IN_TRANSIT -> DELIVERED

#### Inventory
- RECEIVED -> AVAILABLE
- AVAILABLE -> PUTAWAY
- READY -> PUTAWAY

#### WarehouseTask
- CREATED -> STARTED
- CREATED -> PICKING
- CREATED -> PACKING
- PICKING -> IN_PROGRESS
- IN_PROGRESS -> COMPLETED
- PACKING -> COMPLETED
- STARTED -> COMPLETED

#### Order
- CREATED -> ALLOCATED
- CREATED -> RESERVED
- ALLOCATED -> RESERVED
- RESERVED -> PICKING
- PICKING -> PACKED
- PACKED -> SHIPPED
- SHIPPED -> DELIVERED

### 11.4 Validation
- validate_transition(current_state, next_state, entity_type)
- infer_next_state(event_type)
- infer_entity_type(event_type)

### 11.5 Source of truth
The state machine is not distributed across many files; it is centralized in `event_state_machine.py`. This is a good architecture property for consistency.

---

## 12. Database

### 12.1 Schema source
The authoritative database schema is in:
- `database_schema.json`

This is the schema reference used by the project for table names and relationships.

### 12.2 Major tables

#### Products
- products
- product_supplier_mapping

#### Suppliers
- suppliers

#### Procurement
- purchase_orders
- purchase_order_items

#### Shipments
- shipments
- shipment_items
- shipment_transportation
- shipment_tracking
- shipment_loading_events
- shipment_checkpoints

#### Inventory
- inventory
- inventory_locations
- inventory_allocations
- inventory_reservations
- inventory_adjustments
- inventory_transactions
- inventory_snapshots

#### Warehouse operations
- warehouse_tasks
- warehouse_locations
- workers
- workers etc. (attendance/productivity)
- packages

#### Outbound
- orders
- order_items
- outbound_fulfillment
- outbound_shipments
- outbound_shipment_transportation
- outbound_shipment_tracking
- outbound_shipment_loading_events

#### Eventing
- event_outbox

### 12.3 Relationships
Examples from schema:
- purchase_orders.supplier_id -> suppliers.supplier_id
- purchase_orders.warehouse_id -> warehouses.warehouse_id
- purchase_order_items.po_id -> purchase_orders.po_id
- orders.customer_id -> customers.customer_id
- orders.warehouse_id -> warehouses.warehouse_id
- shipments.po_id -> purchase_orders.po_id
- warehouse_tasks.warehouse_id -> warehouses.warehouse_id
- inventory.product_id -> products.product_id
- inventory.warehouse_id -> warehouses.warehouse_id

### 12.4 Which modules read and write which tables

Examples:

- Purchase orders:
  - `purchase_order_created.py` writes purchase_orders and purchase_order_items
  - `purchase_order_approved.py` updates PO state
- Orders:
  - `order_created.py` writes orders
- Shipments:
  - `supplier_shipment_created.py`
  - `transportation` updates shipment stages
- Inventory:
  - `inventory` reads and writes inventory tables
- Tasks:
  - `warehouse` creates and updates warehouse_tasks
- Outbox:
  - `outbox.py`
  - `event_queue.py`

### 12.5 Important contract
This project is tightly coupled to the PostgreSQL schema in `database_schema.json`. The schema is not treated as incidental metadata; it is a real execution contract.

---

## 13. API

### 13.1 Does an API exist?
Yes.

Files:
- `main.py`
- `simulator_routes.py`
- `simulator_service.py`

Framework:
- FastAPI

Routes:
- GET /health
- POST /simulate/full-flow
- GET /simulate/report

### 13.2 Purpose
- health check
- run simulation flow through a request payload
- retrieve latest report JSON

### 13.3 Who calls them
External clients or operators can call them. The code itself does not use the API to drive the internal event loop; it instead runs scripts and returns reports.

### 13.4 Whether simulator uses them
The core simulator uses:
- `master_simulator.py`
- queue/scheduler executor stack

The API is an external control layer, not the central simulation engine.

### 13.5 Production readiness
This API is minimal:
- no auth
- no service layer auth checks
- no session management
- no central telemetry or pipeline
- direct subprocess-driven execution for some flows

This is a lightweight operational API, not a full production ingestion or control plane.

---

## 14. Background services

### 14.1 Continuously running components

I did not find a true continuously running background worker in the codebase.

What exists:
- Airflow DAG in `simulation_trigger_dag.py`
- FastAPI application in `main.py`

What is not found:
- background polling loop
- cron daemon
- worker thread
- scheduler daemon
- event consumer loop

### 14.2 Important distinction
The runtime is event-driven but batch-triggered:
- the queue is ready at a given simulation time
- the loop processes due events
- the system advances checkpoint
- it is not a long-lived queue worker

This is important for understanding the architecture: it is event-driven in semantics, but not a continuously-running event system.

---

## 15. Validation

### 15.1 Current validation framework

The validation framework is a mix of:
- pytest tests
- custom validation utility
- generated report artifact

Files:
- `test_event_scheduler.py`
- `validator_utils.py`
- `run_simulator_validation.py`
- `validation_report.json`

### 15.2 Pytest
The test suite verifies:
- registry contains key event names
- queue due-event behavior
- duplicate event execution
- retry logic
- invalid state transition rejection
- scheduler restart behavior
- master_simulator execution and checkpoint advancement
- validation harness summary

### 15.3 Validation harness
The report in `validation_report.json` includes:
- events_tested
- passed
- failed
- per-domain statuses
- edge-case statuses
- performance summary

### 15.4 Legacy validator
The validator in `validator_utils.py` is relevant but it is a test harness, not the runtime execution layer.

### 15.5 Current coverage
The package includes test modules covering:
- event lifecycle
- handler behavior
- queue semantics
- scheduler semantics
- schema contract
- simulation clock

### 15.6 Limitation
The validation suite is a strong regression harness, but it is still a semantic validation layer above the actual runtime. The queue and executor logic are real, but the validation app is intentionally testing the event contract and not replacing the production runtime itself.

---

## 16. Logging

### 16.1 Logger
File: `logger.py`

This is a simple print-based logger with:
- EVENT : <name>
- formatted details
- TIME
- STATUS : SUCCESS/FAILED

### 16.2 History
EventQueue.execution_history in `event_queue.py` records status and processed time.

### 16.3 Checkpoint
`checkpointing.py` records the last processed simulation time.

### 16.4 Failure logging
Failures are written to:
- print output from logger
- event_outbox status updates
- queue execution_history

### 16.5 Retry logging
Retry counts and next_retry_time are stored in the queued event payload and in the queue history. This is a lightweight operational trace, not a centralized telemetry pipeline.

---

## 17. Current architecture diagram

                     +--------------------------------------+
                     | Airflow DAG                         |
                     | `simulation_trigger_dag.py`    |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | master_simulator.run_simulation_cycle|
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | EventQueue                           |
                     | simulator/core/event_scheduler/      |
                     | `event_queue.py`                       |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | EventScheduler                       |
                     | `scheduler.py`                         |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | EventExecutor                       |
                     | `event_executor.py`                    |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | Event Handler Registry               |
                     | event_handlers/__init__.py           |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | Generator / Business Logic           |
                     | simulator/generators/...             |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | PostgreSQL Database                  |
                     | purchase_orders, shipments, orders,  |
                     | warehouse_tasks, inventory, etc.     |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | event_outbox                         |
                     | core/outbox.py + schema              |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | State Machine                       |
                     | `event_state_machine.py`               |
                     +------------------+-------------------+
                                        |
                                        v
                     +--------------------------------------+
                     | Future Event Creation                |
                     | NEXT_EVENT_SEQUENCE + created_events |
                     +------------------+-------------------+
                                        |
                                        v
                              back to EventQueue

---

## 18. Readiness for Kafka

Important: Kafka is not implemented in the current runtime.

### 18.1 Which components Kafka would replace
If introduced later, the natural replacement would be:
- the outbox-to-dispatch step
- the in-process queue/scheduler handoff
- the direct event consumption model

Currently, the project already has:
- event_outbox as a durable event source
- queue/list_ready_events as the consumer/read side
- scheduler/executor as the processing engine

Kafka would most naturally fit between:
- event_outbox producer
- an external dispatcher/consumer
- downstream processing systems

### 18.2 Which components stay unchanged
The current architecture would still keep:
- generator business logic
- state machine
- database schema
- scheduler/executor logic
- handler contract
- validation harness

### 18.3 Where producer would naturally fit
At the point where a generator calls publish_event in `outbox.py`.

This is the most natural insertion point for a Kafka producer if the project later needs decoupling.

### 18.4 Where consumer would naturally fit
At the point where queue.list_ready_events reads from the source of truth and hands events to executor. A Kafka consumer would replace or sit alongside this read step, but not replace generator logic.

### 18.5 Would scheduler stay?
Most likely yes. The scheduler concept is a valid orchestration component even in a Kafka-based design. The current queue is database-based and in-process, but the scheduler remains a valid dispatch mechanism.

### 18.6 Would executor stay?
Most likely yes. Executor behavior is still needed to interpret event contracts, execute handlers, and enforce state validity.

### 18.7 Would handlers stay?
Yes, for event-specific logic.

### 18.8 Would database change?
Not as a first step. The current PostgreSQL model is still the source of truth for business state and outbox persistence.

### 18.9 Would state machine change?
No, not necessarily. The event/state contract is already centralized and domain-specific.

### 18.10 Would validation change?
Yes, but only to account for the new producer/consumer path. The validation harness is already testing event contracts and should remain useful.

### 18.11 Current recommendation
The project is not Kafka-ready in the sense of operational wiring, but it is structurally ready to add Kafka later as an externalized transport layer, not as a replacement for the business models or scheduler.

---

## 19. What should be implemented next

Based strictly on the existing project, the next implementation order should be:

1. Freeze the event contract and schema contract
   - use `database_schema.json` as the authoritative contract
   - validate event_outbox, aggregate IDs, and payload structure before any transport expansion

2. Review the actual runtime scheduler contract
   - `event_queue.py`
   - `event_executor.py`
   - `master_simulator.py`

3. Review the event generation-to-outbox contract
   - `generators`
   - `outbox.py`
   - `base.py`

4. Review API and orchestration integration
   - `api`
   - `airflow`

5. Decide whether the outbox remains the canonical event source
   - if yes, the next step is operational reliability, not Kafka

6. Only after that, if needed, evaluate Kafka as an external event transport layer
   - not before the business/event contract is fully stable

This order is appropriate because the current architecture already contains the actual business flow; the remaining work is operational oversight and event contract discipline, not redesign.

---

## Final assessment

This project already has a coherent event-driven simulation architecture:

- domain generators write authoritative state
- the outbox records business events
- queue selects ready events
- executor validates and handles them
- handlers return normalized created_events
- the scheduler re-inserts the next future event
- state machine governs valid transitions
- PostgreSQL is the execution and persistence substrate

The architecture is not “microservice architecture” and it is not yet a Kafka-backed streaming system. It is a real, bounded, PostgreSQL-backed event simulator with a clear event lifecycle and scheduler-first orchestration model.

The key architectural strength is that the runtime logic is not scattered; it is concentrated in:
- `master_simulator.py`
- `event_queue.py`
- `event_executor.py`
- `event_state_machine.py`
- `base.py`

That is the precise architecture the project is built around today.