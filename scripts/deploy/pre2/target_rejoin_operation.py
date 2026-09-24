"""Compose authorized rejoin phases with exact native observations.

Author: SqlRush <sqlrush@gmail.com>
No power action, signature, membership decision or database OPEN is performed.
Caller freezes protected configuration and exclusively owns the native target.
"""

from target_command import rejoin_intent
from target_guest import observe_off, observe_running
from target_inventory import resolve_routes
from target_journal import TargetJournalError
from target_permissions import (_access_readback, _access_template, _guard,
                                restore_rejoin_access, revoke_rejoin_access)


def _restore(registry, journal, root, connection, identity, intent, config, deadline):
    state = journal.states.get(identity.operation_id)
    if state is None or state.successor_operation_id:
        raise TargetJournalError("TARGET_REJOIN_NOT_PREPARED")
    node = registry.node(intent.old_node_id)
    if state.rejoin is None:
        # No publication can replace native OFF and old full-route census.
        # The journal atomically validates the exact successor before arming.
        with resolve_routes(root, journal, state.identity, node.bindings):
            observe_off(connection, node.mapping, deadline)
            journal.authorize_rejoin(identity, intent)
    else:
        journal.authorize_rejoin(identity, intent)
    restore_rejoin_access(registry, journal, root, connection, identity, intent, config, deadline)
    return {"status": "ACCESS_READY_UNCERTIFIED"}


def _running(registry, journal, root, connection, identity, intent, config, deadline, phase):
    if phase == "refresh":
        journal.refresh_rejoin(identity, intent)
    node, state = _guard(registry, journal, identity, intent, deadline)
    if (state.rejoin.phase != 3 or state.rejoin.refresh_started != (phase == "refresh")):
        raise TargetJournalError("TARGET_REJOIN_PHASE")
    template = _access_template(registry, journal, root, node, config)
    before = observe_running(connection, node.mapping, deadline)
    _access_readback(root, journal, identity, node, template)
    after = observe_running(connection, node.mapping, deadline)
    _node, final = _guard(registry, journal, identity, intent, deadline)
    if before.runtime_id != after.runtime_id or final != state:
        raise TargetJournalError("TARGET_REJOIN_CHANGED")
    return {"status": "REJOIN_RUNNING_UNCERTIFIED", "runtime_id": after.runtime_id}


def rejoin_operation(document, registry, journal, root, connection, identity, config, deadline):
    """Only called with a decoded, authenticated, registry-bound command.

    Config is the protected owner's fixed native template, never request input.
    A returned status is an observation, not a certified provider result.
    """
    intent, action = rejoin_intent(document), document["action"]
    if action == "rejoin_restore":
        return _restore(registry, journal, root, connection, identity, intent, config, deadline)
    if action == "rejoin_running":
        return _running(registry, journal, root, connection, identity, intent, config,
                        deadline, document["owner_phase"])
    if action == "rejoin_prepare_revoke":
        journal.revoke_rejoin(identity, intent)
        return {"status": "REJOIN_REVOKED"}
    if action != "rejoin_complete_off":
        raise TargetJournalError("TARGET_REJOIN_ACTION")
    _node, state = _guard(registry, journal, identity, intent, deadline)
    if state.rejoin.phase != 4:
        raise TargetJournalError("TARGET_REJOIN_NOT_REVOKED")
    revoke_rejoin_access(registry, journal, root, connection, identity, intent, deadline)
    state = journal._current_rejoin_owner(identity)
    return {"status": "OFF_DRAIN_UNCERTIFIED", "route_phases": list(state.phases)}
