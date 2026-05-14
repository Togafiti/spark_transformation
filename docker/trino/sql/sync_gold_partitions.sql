CALL hive.system.sync_partition_metadata('gold', 'fact_orders', 'FULL');
CALL hive.system.sync_partition_metadata('gold', 'fact_order_items', 'FULL');
CALL hive.system.sync_partition_metadata('gold', 'fact_events', 'FULL');
CALL hive.system.sync_partition_metadata('gold', 'daily_sales', 'FULL');
