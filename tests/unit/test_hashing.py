from recovery_engine.domain.hashing import canonical_json, request_fingerprint


def test_fingerprint_ignores_key_order():
    a = request_fingerprint("issue_invoice", {"amount": 10, "customer": {"id": 1, "tier": "a"}})
    b = request_fingerprint("issue_invoice", {"customer": {"tier": "a", "id": 1}, "amount": 10})
    assert a == b


def test_fingerprint_changes_with_payload():
    assert request_fingerprint("t", {"amount": 10}) != request_fingerprint("t", {"amount": 11})


def test_fingerprint_changes_with_operation_type():
    assert request_fingerprint("a", {"x": 1}) != request_fingerprint("b", {"x": 1})


def test_canonical_json_is_compact_and_sorted():
    assert canonical_json({"b": 1, "a": "ñ"}) == '{"a":"ñ","b":1}'
