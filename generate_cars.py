import functools
import json
import gzip
from datetime import datetime
from collections import defaultdict, Counter
import pandas as pd
import os
from pathlib import Path
import contextlib
import itertools
import more_itertools
from hashlib import md5
import argparse
import ir_datasets
from lz4.frame import decompress, LZ4FrameFile

_logger = ir_datasets.log.easy()


class Sessionizer:
    def __init__(self, glove_model='glove-wiki-gigaword-50', threshold=0.5):
        import gensim.downloader
        self.prev_prefix = None
        self.prev_repr = None
        self.prev_session_id = None
        self.prev_qid = None
        self.user_session_counter = Counter()
        with _logger.duration(f'loading {glove_model}'):
            self.glove = gensim.downloader.load(glove_model)
        self.threshold = threshold

    def next_session_id(self, qid, query, prefix):
        sim, query_repr = self.get_sim_repr(qid, query, prefix)
        if sim > self.threshold:
            session_id = self.prev_session_id
        else:
            session_id = f'{prefix}_{self.user_session_counter[prefix]}'
            self.user_session_counter[prefix] += 1
        self.prev_user_id = prefix
        self.prev_repr = query_repr
        self.prev_session_id = session_id
        self.prev_qid = qid
        self.prev_prefix = prefix
        return session_id

    def get_sim_repr(self, qid, query, prefix):
        np = ir_datasets.lazy_libs.numpy()
        if self.prev_prefix != prefix:
            return 0., None
        if self.prev_qid == qid:
            return 1., self.prev_repr
        vecs = [self.glove.get_vector(t.lower()) for t in query.split() if t.lower() in self.glove]
        if vecs:
            query_repr = np.stack(vecs).mean(axis=0)
            if self.prev_repr is not None:
                return np.dot(query_repr, self.prev_repr)/(np.linalg.norm(query_repr)*np.linalg.norm(self.prev_repr)), query_repr
            return 0., query_repr
        return 0., None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_dir', type=Path, required=True)
    parser.add_argument('--dataset', default='aol-ia')
    parser.add_argument('--splits', nargs='+', default='train:2006-04-08:2006-05-17:5 dev:2006-05-17:2006-05-24:5 test:2006-05-24:2007-01-01:50'.split())
    parser.add_argument('--run', required=True)
    parser.add_argument('--trial', action='store_true')
    parser.add_argument('--title_only', action='store_true')
    args = parser.parse_args()

    splits = {}
    for split in args.splits:
        cols = split.split(':')
        if len(cols) == 3:
            name, start, end, context = *cols, 50
        elif len(cols) == 4:
            name, start, end, context = cols
        splits[name] = (datetime.fromisoformat(start), datetime.fromisoformat(end), int(context))

    dataset = ir_datasets.load(args.dataset)

    queries_todo = {}
    logs_by_id = defaultdict(list)
    qlogs_cls = dataset.qlogs_cls()
    field = next(f for f in ['session_id', 'user_id'] if f in qlogs_cls._fields)
    field = qlogs_cls._fields.index(field)

    qlogs = dataset.qlogs

    if args.trial:
        qlogs = itertools.islice(qlogs, 100_000)

    for qlog in _logger.pbar(qlogs, desc='grouping logs'):
        logs_by_id[qlog[field]].append(qlog)

    sessionizer = Sessionizer()
    sessions = defaultdict(list)
    for prefix, logs in _logger.pbar(logs_by_id.items(), desc='building sessions'):
        logs = sorted(logs, key=lambda x: x.time)
        for log in logs:
            session_id = sessionizer.next_session_id(log.query_id, log.query, prefix)
            sessions[session_id].append(log)

    del logs_by_id

    with _logger.duration('filtering sessions'):
        sessions = {sid: logs for sid, logs in sessions.items() if len(set(l.query_id for l in logs)) > 1}

    if not args.out_dir.exists():
        args.out_dir.mkdir(exist_ok=True, parents=True)

    ds = dataset.docs_store()

    runs_by_did = defaultdict(dict)
    with _logger.duration(f'reading run {args.run}'), gzip.open(args.run, 'rb') as fin:
        for line in fin:
            qid, _, did, rank, _, _ = line.decode().strip().split()
            runs_by_did[str(qid)][str(did)] = int(rank)

    with contextlib.ExitStack() as stack:
        split2file = {split: stack.enter_context(open(args.out_dir/f'{split}.json', 'wt')) for split in splits}
        for session_id, logs in _logger.pbar(sorted(sessions.items()), desc='creating records'):
            # make sure it appears in a split
            split = [s for s, (start, end, _) in splits.items() if start <= logs[0].time < end]
            if len(split) == 0:
                continue
            split = split[0]
            context = splits[split][2]
            record = {
                'session_id': session_id,
                'query': []
            }
            for log in logs:
                # matching_query = [q for q in record['query'] if q['text'] == log.query]
                clicked_dids = {i.doc_id for i in log.items if i.clicked}
                if args.run:
                    dids = get_dids_from_run(runs_by_did[log.query_id], clicked_dids, context)
                else:
                    dids = [i.doc_id for i in log.items]
                docs = ds.get_many(dids)
                query = {
                    'id': str(len(record['query'])),
                    'text': log.query,
                    'tokens': log.query.split(),
                    'candidates': []
                }
                record['query'].append(query)
                for doc_id in dids:
                    if doc_id not in docs:
                        continue
                    doc = docs[doc_id]
                    query['candidates'].append({
                        'id': doc_id,
                        'title': doc.title,
                        'content': '' if args.title_only else doc.text,
                        'url': doc.url,
                        'label': doc_id in clicked_dids,
                    })
            # ensure each query has at least one clicked and one non-clicked document
            record['query'] = [q for q in record['query'] if any(c['label'] for c in q['candidates']) and any(not c['label'] for c in q['candidates'])]
            # make sure the session has more than 1 query
            if len(record['query']) > 1:
                split2file[split].write(json.dumps(record) + '\n')


def get_dids_from_run(run_by_did, clicked_dids, context):
    run_by_rank = {rank: did for did, rank in run_by_did.items()}
    target_dids = set()
    for did in clicked_dids:
        if did in run_by_did:
            rank = run_by_did[did]
            start, stop = rank - context // 2, rank + context // 2
            if stop - start != context: # if odd
                stop += 1
            while start not in run_by_rank:
                start += 1
                stop += 1
            while stop - 1 not in run_by_rank:
                start -= 1
                stop -= 1
            while start not in run_by_rank:
                start += 1
            for rank in range(start, stop):
                if rank in run_by_rank:
                    target_dids.add(run_by_rank[rank])
    return sorted(target_dids)


if __name__ == "__main__":
    main()
