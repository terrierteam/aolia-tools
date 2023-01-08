import requests
import ntlk
from nltk import word_tokenize
import contextlib
import fcntl
import time
import multiprocessing
import os
import sys
from pathlib import Path
import gzip
import hashlib
import json
import pickle
import argparse
import ir_datasets
import io
import re
import chardet
import codecs
from lxml.html import etree
from lz4 import frame
nltk.download('punkt')

def sax_html_parser(body):
    sax = SaxExtractor()
    parser = etree.HTMLParser(target=sax)
    if isinstance(body, bytes):
        encoding = chardet.detect(body)['encoding'] or 'utf8'
        cdc = codecs.lookup(encoding)
        while body:
            text, count = cdc.decode(body, 'ignore')
            parser.feed(text)
            body = body[count:]
    else:
        parser.feed(body)
    parser.close()
    return sax.get_title(), str(sax)


class SaxExtractor:
    IGNORE_TAGS = {'noscript', 'meta', 'input', 'script', 'style'}
    def __init__(self):
        self.title = io.StringIO()
        self.text = io.StringIO()
        self.in_title = False
        self.ignore_tag_stack = []
    def __str__(self):
        self.text.seek(0)
        return self.text.read()
    def get_title(self):
        self.title.seek(0)
        return self.title.read()
    def data(self, data):
        if not self.ignore_tag_stack:
            if self.in_title:
                self.title.write(data)
            else:
                self.text.write(data)
    def start(self, tag, attrs):
        tag = tag.lower()
        if tag.lower() in self.IGNORE_TAGS:
            self.ignore_tag_stack.append(tag)
        if tag.lower() == 'title':
            self.in_title = True
    def end(self, tag):
        tag = tag.lower()
        if tag in self.IGNORE_TAGS:
            while self.ignore_tag_stack and self.ignore_tag_stack.pop() != tag:
                pass
        if tag.lower() == 'title':
            self.in_title = False
    def close(self):
        pass
    def comment(self, data):
        pass
    def doctype(self, *args):
        pass
    def pi(self, *args):
        pass


_logger = ir_datasets.log.easy()

_session = None
def start():
    global _session
    _session = requests.Session()

def worker(args):
    global _session
    docid, (url, wb_url), path = args
    # I've seen that in rare situations, data can get mangled by intermediate parties if we use
    # the http endpoint instead of the https endpoint.
    wb_url = wb_url.replace('http://web.archive.org/web', 'https://web.archive.org/web')
    try:
        resp = _session.get(wb_url, stream=True, timeout=15)
        resp.raise_for_status()
        if 'html' not in resp.headers.get('content-type', '').lower():
            raise ValueError(f'content-type {resp.headers.get("content-type")}')
        resp.raw.decode_content = True
        raw_body = resp.raw.read()
        title, text = sax_html_parser(raw_body)
        title = ' '.join(word_tokenize(title))
        text = ' '.join(word_tokenize(text))
        if title == '502 Bad Gateway':
            # This isn't raised by raise_for_status for some reason, but it means archive.org is booting us
            # There's a few exceptions where this is actually the expected title, don't raise for those.
            if not (docid in ('80445ed4fc45', '9ff7c85c8c28', '03e460cf3fa1') and text == '502 Bad Gateway nginx'):
                raise RuntimeError('502 bad gateway')
        with (path/f'{docid[0]}.jsonl.lz4').open('ab') as fout:
            try:
                fcntl.lockf(fout, fcntl.LOCK_EX) # wait until this file is available
                fout.seek(0, 2) # seek to the end
                fout.write(frame.compress(json.dumps({
                    'doc_id': docid,
                    'url': url,
                    'wb_url': wb_url,
                    'title': title,
                    'text': text,
                }).encode() + b'\n'))
            finally:
                fcntl.lockf(fout, fcntl.LOCK_UN)
        return docid, None
    except Exception as ex:
        ex = str(ex)
        if '404 Client Error: NOT FOUND' in ex:
            ex = '404 not found'
        elif '403 Client Error: FORBIDDEN' in ex:
            ex = '403 forbidden'
        elif '[Errno 111] Connection refused' in ex:
            ex = 'connection refused'
        elif 'Read timed out' in ex:
            ex = 'read timed out'
        return docid, f'failed download of {wb_url}: {ex}; will retry'


def main():
    with contextlib.ExitStack() as outer_stack:
        parser = argparse.ArgumentParser(prog='aol-ia downloader', description='Downloads documents for the AOL-IA dataset from archive.org')
        parser.add_argument('--source', type=Path, help='Path to the source aol.id2wb.tsv.gz file. If not provided, this will pull from ir-datasets.')
        parser.add_argument('--path', type=Path, help='Output directory path of the downloaded files. If not provided, this will use the directory from ir-datasets.')
        parser.add_argument('--parallel', default=10, type=int, help='Number of worker processes. Setting this value too high will cause frequent rate limits and increase the overall download time.')
        parser.add_argument('--backoff_threshold', default=10, type=int, help='Number of consecutive errors that will trigger a backoff.')
        parser.add_argument('--backoff_duration', default=10., type=float, help='Amount of time to wait after a backoff was triggered (in seconds).')
        args = parser.parse_args()
        assert 1 <= args.parallel

        if args.source is not None:
            args.source = outer_stack.enter_context(gzip.open(args.source))
        if args.source is None:
            args.source = outer_stack.enter_context(ir_datasets.datasets.aol_ia.MANAGER.id2wb_dlc.stream())

        if args.path is None:
            args.path = ir_datasets.datasets.aol_ia.PATH/'downloaded_docs'

        done_ids = set()
        todo = set()
        in_progress = set()
        notfound = set()

        if not args.path.exists():
            args.path.mkdir(exist_ok=True, parents=True)

        with _logger.duration('preparing to download...'):
            if not args.path.exists():
                args.path.mkdir(exist_ok=True, parents=True)
            if (args.path/'done_ids.txt').exists():
                with (args.path/'done_ids.txt').open('rt') as fin:
                    for line in fin:
                        done_ids.add(line.strip())
            did2url = {}
            for line in args.source:
                did, url, wb_url = line.decode().rstrip('\n').split('\t')
                did2url[did] = (url, wb_url)
                if did not in done_ids:
                    todo.add(did)

        total_reqests = 0
        total_done = 0
        start_time = time.time()
        with _logger.pbar_raw(desc='downloading aol docs', unit='requests') as pbar:
            try:
                while todo:
                    failures_in_a_row = 0
                    with contextlib.ExitStack() as stack:
                        f_done = stack.enter_context((args.path/'done_ids.txt').open('at'))
                        try:
                            fcntl.lockf(f_done, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except OSError:
                            _logger.warn(f'Could not get exclusive access to {(args.path/"done_ids.txt")}. There may only be one downloader running at a time. Set --parallel to increase the number of worker processes.')
                            sys.exit(-1)
                        def doc_iter():
                            while todo or in_progress:
                                if todo:
                                    did = todo.pop()
                                    in_progress.add(did)
                                    yield did, did2url[did], args.path
                                else:
                                    time.sleep(1.) # wait 'till remaining items in_progress succeed (exiting loop) or fail (which moves it to todo)
                        if args.parallel == 1:
                            _logger.warn('starting with 1 process; consider setting --parallel with a higher value to speed up download process.')
                            start()
                            mapper = map
                        else:
                            pool = stack.enter_context(multiprocessing.Pool(args.parallel, start))
                            mapper = pool.imap_unordered
                        for docid, error in mapper(worker, doc_iter()):
                            if error is None:
                                f_done.write(f'{docid}\n')
                                done_ids.add(docid)
                                failures_in_a_row = 0
                                total_done += 1
                            elif '404 not found' in error or '403 forbidden' in error:
                                f_done.write(f'{docid}\n') # mark it as done, as we won't be able to get it
                                done_ids.add(docid)
                                notfound.add(docid)
                                failures_in_a_row = 0
                                total_done += 1
                            else:
                                _logger.info(error)
                                todo.add(docid)
                                failures_in_a_row += 1
                                if failures_in_a_row >= args.backoff_threshold:
                                    pool.terminate()
                                    todo.update(in_progress)
                                    in_progress.clear()
                                    _logger.info(f'{failures_in_a_row} failures in a row; backing off for {args.backoff_duration}sec...')
                                    time.sleep(args.backoff_duration)
                                    break
                            in_progress.discard(docid)
                            total_reqests += 1
                            overall_rate = total_reqests / (time.time() - start_time)
                            est_remaining_requests = (len(todo) + len(in_progress)) / (total_done/total_reqests+1e-5)
                            est_remaining_time = est_remaining_requests / overall_rate
                            pbar.set_postfix({
                                'todo': str(len(todo) + len(in_progress)),
                                'done': str(len(done_ids)),
                                'done%': '{:.2f}%'.format(len(done_ids)/len(did2url)*100),
                                'notfond': str(len(notfound)),
                                'success_rate': '{:.1f}%'.format(total_done/total_reqests*100),
                                'est_remaining': ir_datasets.log.format_interval(est_remaining_time),
                            }, refresh=False)
                            pbar.update()

            except KeyboardInterrupt:
                _logger.info('KeyboardInterrupt')
            else:
                (args.path/'_done').touch()

if __name__ == '__main__':
    main()
