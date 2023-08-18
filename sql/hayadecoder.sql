SELECT slot_name FROM pg_create_logical_replication_slot('custom_slot', 'hayadecoder');

SELECT slot_name, plugin, slot_type FROM pg_replication_slots WHERE slot_name = 'custom_slot';

CREATE TABLE foo (id int);

BEGIN;
INSERT INTO foo VALUES (1);
UPDATE foo SET id = 2 WHERE id = 1;
DELETE FROM foo;
COMMIT;

SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'write_txd', 'off');
