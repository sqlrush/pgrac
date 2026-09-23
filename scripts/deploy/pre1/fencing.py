"""Verify exact storage-fence mappings and scratch-writer observations.

Author: SqlRush <sqlrush@gmail.com>

These checks neither perform power operations nor create a database certificate.
The caller must retain command results and independently observe the target UUID.
"""

import json
import re
import xml.etree.ElementTree as ET

from common import PreflightError


def nvpairs(element):
    result = {}
    if element.find(".//rule") is not None:
        raise PreflightError("CONDITIONAL_FENCE_CONFIGURATION")
    for item in element.findall(".//nvpair"):
        key, value = item.get("name"), item.get("value")
        if key is None or value is None or key in result:
            raise PreflightError("AMBIGUOUS_FENCE_CONFIGURATION")
        result[key] = value
    return result


def verify_configuration(xml, resource_id, expected_map):
    if (type(expected_map) is not dict or len(expected_map) != 4
            or len(set(expected_map.values())) != 4
            or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name)
                   or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", domain)
                   for name, domain in expected_map.items())):
        raise PreflightError("FENCE_MAPPING_INVALID")
    if len(xml) > 4 * 1024 * 1024 or "<!" in xml:
        raise PreflightError("FENCE_CONFIGURATION_INVALID")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        raise PreflightError("FENCE_CONFIGURATION_INVALID") from None
    properties = root.find("./configuration/crm_config")
    if properties is None:
        raise PreflightError("FENCE_CONFIGURATION_INVALID")
    props = nvpairs(properties)
    for name, value in {"stonith-enabled": "true", "stonith-action": "off",
                        "no-quorum-policy": "freeze", "maintenance-mode": "false"}.items():
        if props.get(name) != value:
            raise PreflightError("FENCE_POLICY_MISMATCH")
    resources = [r for r in root.findall("./configuration/resources//primitive")
                 if r.get("class") == "stonith"]
    if (len(resources) != 1 or resources[0].get("id") != resource_id
            or resources[0].get("type") != "fence_virsh"):
        raise PreflightError("FENCE_RESOURCE_MISMATCH")
    resource = resources[0]
    if resource.find(".//rule") is not None or resource.get("template"):
        raise PreflightError("CONDITIONAL_FENCE_CONFIGURATION")
    attributes = resource.findall("./instance_attributes")
    if len(attributes) != 1:
        raise PreflightError("AMBIGUOUS_FENCE_CONFIGURATION")
    attrs = nvpairs(attributes[0])
    # DLM can request reboot independently of the cluster's default off policy.
    # The profile must map that action to off too; live OFF observation remains
    # mandatory, since an accepted configuration is not an isolation witness.
    if (attrs.get("pcmk_reboot_action") != "off"
            or attrs.get("pcmk_off_action", "off") != "off"
            or attrs.get("pcmk_host_argument", "port") != "port"
            or {"action", "port", "plug"} & set(attrs)):
        raise PreflightError("FENCE_ACTION_OR_TARGET_OVERRIDE")
    if (attrs.get("pcmk_host_check") != "static-list"
            or attrs.get("missing_as_off", "false") not in ("false", "0")
            or set(attrs.get("pcmk_host_list", "").split()) != set(expected_map)):
        raise PreflightError("FENCE_TARGET_POLICY_MISMATCH")
    actual = {}
    for pair in attrs.get("pcmk_host_map", "").split(";"):
        name, separator, domain = pair.partition(":")
        if not separator or name in actual or name not in expected_map or domain != expected_map[name]:
            raise PreflightError("FENCE_MAPPING_MISMATCH")
        actual[name] = domain
    if actual != expected_map:
        raise PreflightError("FENCE_MAPPING_MISMATCH")
    return actual


def domain_state(rc, output):
    if type(rc) is not int or rc != 0 or type(output) is not str:
        raise PreflightError("DOMAIN_STATUS_UNAVAILABLE")
    value = output.strip()
    if re.fullmatch(r"shut off(?: \((?:destroyed|shutdown)\))?", value):
        return "OFF"
    if re.fullmatch(r"running(?: \((?:booted|unpaused|restored|migrated|wakeup|unknown)\))?", value):
        return "ON"
    raise PreflightError("DOMAIN_STATUS_UNPROVEN")


def require_database_empty(observations):
    try:
        if len(observations) != 4 or {r["node_id"] for r in observations} != set(range(4)):
            raise ValueError
        for result in observations:
            facts = result["observation"]
            if (result["status"] != "PASS" or facts["node_id"] != result["node_id"]
                    or facts["pgdata_state"] not in ("EMPTY", "ABSENT")
                    or facts["control"] is not None or facts["pidfile"] is not None
                    or facts["processes"] != []):
                raise ValueError
    except (ValueError, TypeError, KeyError):
        raise PreflightError("DATABASE_EMPTY_PRECONDITION_UNPROVEN") from None


def probe_events(raw, token, node):
    if type(raw) is not str or len(raw) > 4 * 1024 * 1024:
        raise PreflightError("WRITER_EVIDENCE_INVALID")
    try:
        events = [json.loads(line) for line in raw.splitlines()]
        keys = {"case", "syscall", "result", "errno", "offset", "length", "crc32", "token",
                "node", "mono_ns", "status"}
        for event in events:
            if (type(event) is not dict or set(event) != keys
                    or event["token"] != token or type(event["node"]) is not int or event["node"] != node
                    or any(type(event[k]) is not int for k in ("result", "errno", "offset", "length", "crc32", "mono_ns"))
                    or event["status"] not in ("OK", "READY", "SYNCED", "PASS")
                    or event["errno"] != 0 or event["mono_ns"] <= 0):
                raise ValueError
        if not events:
            raise ValueError
        return events
    except (ValueError, TypeError, KeyError):
        raise PreflightError("WRITER_EVIDENCE_INVALID") from None


def writer_progress(raw, token, node):
    events = probe_events(raw, token, node)
    progress = [e for e in events if e["syscall"] == "fence-progress"]
    if (any(e["case"] != "fence-writer" for e in events) or len(progress) < 2
            or progress[0]["status"] != "READY"
            or any(e["status"] != "SYNCED" for e in progress[1:])
            or any(e["length"] != 8192 or e["result"] <= 0 for e in progress)
            or any(b["result"] != a["result"] + 1 or b["mono_ns"] <= a["mono_ns"]
                   for a, b in zip(progress, progress[1:]))):
        raise PreflightError("WRITER_PROGRESS_UNPROVEN")
    return progress[-1]["result"]


def observed_payload(raw, token, reader):
    events = probe_events(raw, token, reader)
    if (events[-1]["syscall"] != "complete" or events[-1]["status"] != "PASS"
            or any(e["case"] != "observe" for e in events)):
        raise PreflightError("PAYLOAD_OBSERVATION_INCOMPLETE")
    wanted = {}
    for event in events:
        if event["syscall"] in ("verify", "observed-version", "observed-writer"):
            if event["syscall"] in wanted:
                raise PreflightError("PAYLOAD_OBSERVATION_INVALID")
            wanted[event["syscall"]] = event
    if (set(wanted) != {"verify", "observed-version", "observed-writer"}
            or wanted["verify"]["result"] != 0 or wanted["verify"]["length"] != 8192
            or wanted["observed-version"]["result"] <= 0
            or not 0 <= wanted["observed-writer"]["result"] <= 3):
        raise PreflightError("PAYLOAD_OBSERVATION_INVALID")
    return (wanted["observed-version"]["result"], wanted["observed-writer"]["result"],
            wanted["verify"]["crc32"])
