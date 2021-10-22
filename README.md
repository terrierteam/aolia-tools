# AOLIA-tools

This repository provides tools for working with the AOLIA corpus: a version of the documents from
the AOL query log that can be scraped from [The Internet Archive](https://archive.org/), representing
documents close to how they appeared at the time the log was created.

## Getting Started

Clone this repository and install dependencies:

```bash
git clone https://github.com/terrierteam/aolia-tools
cd aolia-tools
pip install -r requirements.txt
```

## Downloading the Corpus

The `downloader.py` script downloads the documents for AOLIA from The Internet Archive.
Downloads are done in parallel and the process takes about 2 days. The software automatically
backs off when it detects rate limiting.

There are two ways you can run the download script. If you are using the [`ir-datasets`](https://ir-datasets.com/),
package, you can simply run:

```bash
python downloader.py
```

This will automatically configure the script to work with `ir-datasets`.

If you do not want to use `ir-datsets`, you can specify the location of the `aol.id2wb.tsv.gz` file
(downloadable here: https://macavaney.us/aol.id2wb.tsv.gz, MD5: `afbf9b03e1a0fabc9f3fdd5105e6ae5a`)
using the `--source` argument and the output directory for the downloaded files using the `--path` argument.

```bash
wget https://macavaney.us/aol.id2wb.tsv.gz
python downloader.py --source aol.id2wb.tsv.gz --path output_docs
```

The output directory will contain 16 files, split by the first character of the document IDs.
Each contains json-lines data and is encoded using lz4 compression.

For both settings, you can specify `--parallel` to change how many worker processes are used (default: 10),
`--backoff_threshold` to change how many consecutive errors that will trigger a backoff (default: 10), and 
`--backoff_duration` to change how long a backoff waits until it starts going again, in seconds (default: 10).
We found these settings to work well on our network.

## Building CARS Datasets

You can use the the `generate_cars.py` script to generate input files that are usable by
[wasiahmad/context_attentive_ir](https://github.com/wasiahmad/context_attentive_ir), allowing you to
run baselines like CARS, M-NSRF, and M-MatchTensor.

The script has two required argument: `--out_dir`, which specifies the directory to which to save the
dataset files, and `--run`, which specifies the gzip'd TREC-formatted run file for all AOL queries.
This file can be downloaded [here](https://drive.google.com/file/d/1dYval2CG8V98RoL6D2gwJ2vqPM44yf-i/view?usp=sharing)
(1.3GB, MD5: `d464f3703384ddfca5c08ae4892c4400`).

Right now, this script only works if you are using `ir-datasets`.

```bash
python generate_cars.py --out_dir path/to/context_attentive_ir/data/aolia --run path/to/aolia-title-bm25.run.partial.gz
```

## Replacing CAR-formmated Dataset Titles

To reproduce the `Corpus=AOL17, Docs=AOLIA` setting, we provide the `replace_cars_titles.py` script. This
takes as input a CARS-formatted dataset file and outputs a new file that replaces the document texts
with those from `AOLIA`.

Right now, this script only works if you are using `ir-datasets`.

```bash
python replace_cars_titles.py path/to/output/split.json path/to/output/split.json
```
