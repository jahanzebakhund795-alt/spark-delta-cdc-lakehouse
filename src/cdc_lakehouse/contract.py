import json

def validate(row):
    if not isinstance(row,dict):
        raise ValueError('Expected object')
    if not isinstance(row.get('order_id'),str) or not row['order_id']:
        raise ValueError('order_id required')
    if type(row.get('version')) is not int or row['version']<1:
        raise ValueError('Positive integer version required')
    if row.get('op') not in ('upsert','delete'):
        raise ValueError('Unsupported operation')
    if row['op']=='upsert' and (type(row.get('amount_cents')) is not int or row['amount_cents']<0):
        raise ValueError('Nonnegative amount required')
    return {'order_id':row['order_id'],'version':row['version'],'op':row['op'],
            'region':row.get('region') or 'UNKNOWN',
            'amount_cents':row.get('amount_cents',0) if row['op']=='upsert' else 0,
            'is_deleted':row['op']=='delete'}

def deduplicate(rows):
    versions,latest,rejected = {},{},[]
    for index,row in enumerate(rows):
        try:
            clean=validate(row)
        except ValueError as error:
            rejected.append({'index':index,'record':row,'reason':str(error)})
            continue
        key=(clean['order_id'],clean['version'])
        if key in versions and versions[key]!=clean:
            raise ValueError('Conflicting payloads at identical source version')
        versions[key]=clean
        old=latest.get(clean['order_id'])
        if old is None or old['version']<clean['version']:
            latest[clean['order_id']]=clean
    return list(latest.values()),rejected

def merge_reference(state,rows):
    updates,rejected=deduplicate(rows)
    result={k:dict(v) for k,v in state.items()}
    for row in updates:
        old=result.get(row['order_id'])
        if old is None or row['version']>old['version']:
            result[row['order_id']]=row
        elif row['version']==old['version'] and row!=old:
            raise ValueError('Conflicting existing version')
    return result,rejected
