import argparse
import json
import ir_datasets

_logger = ir_datasets.log.easy()

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('source')
  parser.add_argument('dest')
  args = parser.parse_args()
  url_to_title = {}
  for doc in _logger.pbar(ir_datasets.load('aol-ia').docs, desc='loading docs'):
    url_to_title[doc.url] = doc.title
  with open(args.source, 'rt') as fin, open(args.dest, 'wt') as fout:
    for line in fin:
      data = json.loads(line)
      for query in data['query']:
        for click in query['clicks']:
          # prioritize exact URL matches, otherwise fall back on version with end slash removed
          # (since some versions of the CARS data adds a trailing slash), otherwise fall back on empty string
          title = url_to_title.get(click['url'], url_to_title.get(click['url'].rstrip('/'), ''))
          click['title'] = title
      fout.write(json.dumps(data) + '\n')

if __name__ == '__main__':
  main()
