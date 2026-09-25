import sys
sys.path.insert(0, r'C:\Users\rohit\PycharmProjects\daemon-supply-chain\simulator')
from core.event_scheduler.event_queue import EventQueue
from core.event_scheduler.event_executor import EventExecutor

queue = EventQueue(memory_mode=True)
executor = EventExecutor(queue=queue, memory_mode=True)

# Duplicate case
row = {
    'event_id': 'abc',
    'event_type': 'PurchaseOrderApproved',
    'aggregate_type': 'purchase_orders',
    'aggregate_id': 'PO-2001',
    'correlation_id': 'corr-2001',
    'payload': {
        'po_id': 'PO-2001',
        'entity_type': 'PurchaseOrder',
        'current_state': 'CREATED',
        'next_state': 'APPROVED',
        'priority': 'HIGH',
    },
}
first = executor.execute(row, registry={'PurchaseOrderApproved': lambda context: {'status': 'SUCCESS', 'created_events': []}})
second = executor.execute(row, registry={'PurchaseOrderApproved': lambda context: {'status': 'SUCCESS', 'created_events': []}})
print('duplicate first=', first)
print('duplicate second=', second)
print('execution log=', executor._execution_log)
print('queue rows=', queue._memory_events)

# Missing correlation case
queue2 = EventQueue(memory_mode=True)
executor2 = EventExecutor(queue=queue2, memory_mode=True)
row2 = {
    'event_id': 'def',
    'event_type': 'OrderCreated',
    'aggregate_type': 'orders',
    'aggregate_id': 'ORD-1',
    'correlation_id': None,
    'payload': {
        'order_id': 'ORD-1',
        'entity_type': 'Order',
        'current_state': 'CREATED',
        'next_state': 'ALLOCATED',
        'priority': 'MEDIUM',
        'correlation_id': None,
    },
}
result = executor2.execute(row2, registry={'OrderCreated': lambda context: {'status': 'SUCCESS', 'created_events': []}})
print('missing correlation=', result)
print('execution log 2=', executor2._execution_log)
