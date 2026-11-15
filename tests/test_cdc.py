import pytest
from cdc_lakehouse.contract import merge_reference,deduplicate

def row(version,op='upsert',amount=100):
    return {'order_id':'A','version':version,'op':op,'amount_cents':amount,'region':'WEST'}

def test_tombstone_prevents_stale_resurrection():
    state,_=merge_reference({},[row(1),row(3,'delete'),row(2)])
    assert state['A']['is_deleted']
    replay,_=merge_reference(state,[row(2),row(3,'delete')])
    assert replay==state
    restored,_=merge_reference(state,[row(4)])
    assert not restored['A']['is_deleted']

def test_version_conflicts_fail_before_merging():
    with pytest.raises(ValueError):
        deduplicate([row(1),row(1,amount=999)])
    state,_=merge_reference({},[row(1)])
    with pytest.raises(ValueError):
        merge_reference(state,[row(1,amount=101)])
    assert state['A']['amount_cents']==100

def test_quarantine_and_deduplication():
    updates,rejected=deduplicate([row(1),row(1),row(0),row(2,amount=-3)])
    assert len(updates)==1 and len(rejected)==2
