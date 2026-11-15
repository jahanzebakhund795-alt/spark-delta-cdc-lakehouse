import json
from pathlib import Path
from .contract import merge_reference

def main():
    state={}
    for name in ['batch1.jsonl','batch2.jsonl']:
        rows=[json.loads(line) for line in (Path('fixtures')/name).read_text().splitlines()]
        state,rejected=merge_reference(state,rows)
        print(json.dumps({'batch':name,'state':state,'rejected':rejected},indent=2))
if __name__=='__main__':
    main()
