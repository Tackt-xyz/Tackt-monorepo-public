SET LOCAL statement_timeout = 0;
SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM core_actions WHERE risk_evaluation_id IS NOT NULL) THEN
    RAISE EXCEPTION 'cannot remove synchronous Risk evaluation audit evidence';
  END IF;
  IF EXISTS (SELECT 1 FROM core_action_ledger_requests)
     OR EXISTS (SELECT 1 FROM core_actions WHERE core_action_state_is_active(state)) THEN
    RAISE EXCEPTION 'Core actions contain durable work; restore a pre-migration backup instead';
  END IF;
END $$;

DROP TABLE core_action_ledger_requests;
DROP TYPE core_action_ledger_step;
DROP TABLE core_action_transitions;
DROP FUNCTION reject_core_action_audit_mutation();
DROP TABLE core_actions;
DROP FUNCTION core_action_state_is_active(core_action_state);
DROP TYPE core_action_state;
DROP TYPE core_action_kind;
