import argparse
from concurrent.futures import ProcessPoolExecutor
import math
import os
import shutil

import tabula
import fitz # from pymupdf
import pandas as pd


def main():
    # experimental results suggest using half of cpus is optimal
    optimal_cores = os.cpu_count() // 2

    # parse args
    parser = argparse.ArgumentParser(description="Parallel PDF table parser.")
    parser.add_argument("--num-cores", type=int, default=optimal_cores)
    parser.add_argument("--pdf-name", default="EOD_FUTURES_REPORT.PDF")
    parser.add_argument("--split-dir", default="split")
    args = parser.parse_args()

    print(f"splitting into {args.num_cores} files...")
    split_pdf(args.pdf_name, args.num_cores, args.split_dir)

    print(f"parsing on {args.num_cores} cores...")
    split_paths = sorted([os.path.join(args.split_dir, f) for f in os.listdir(args.split_dir) if f.endswith(".pdf")])
    with ProcessPoolExecutor(max_workers=args.num_cores) as executor:
        res = list(executor.map(parse_pdf, split_paths))
    
    # remove split dir
    if os.path.isdir(args.split_dir):
        old_files = os.listdir(args.split_dir)
        for file in old_files:
            if file.endswith(".pdf"):
                fpath = os.path.join(args.split_dir, file)
                os.remove(fpath)
        os.rmdir(args.split_dir)

    print("merging...")
    merged = pd.concat(res, ignore_index=True)
    
    # cleanup headers
    merged.columns = [col.replace('\r', ' ').replace('*', '') for col in merged.columns]

    # either use dataframe or save as csv/xlsx
    print(merged)
    #merged.to_csv("merged.csv", index=False)
    #merged.to_excel("merged.xlsx", index=False)


def split_pdf(fname: str, num_files: int, out_dir: str):
    """Split PDF fname into num_files. Outputs in out_dir.

    Args:
        fname (str): File name.
        num_files (int): How many split files there should be.
        out_dir (str): Where the split files go.
    """
    doc = fitz.open(fname)
    pages_per_split = math.ceil(len(doc) / num_files)

    # add zero paddings so files can be sorted correctly
    padding = len(str(num_files - 1))
    os.mkdir(out_dir)

    for i in range(num_files):
        split_doc = fitz.open()
        begin_page = i * pages_per_split
        last_page = min(begin_page + pages_per_split - 1, len(doc) - 1) # inclusive

        split_doc.insert_pdf(doc, from_page=begin_page, to_page=last_page)

        split_fname = f"{str(i).zfill(padding)}.pdf"
        split_fpath = os.path.join(out_dir, split_fname)
        split_doc.save(split_fpath)
        split_doc.close()


def parse_pdf(fpath: str) -> pd.DataFrame:
    """Parse PDF and return as dataframe.

    Args:
        fpath (str): File path.

    Returns:
        pd.DataFrame: Table parsed as a dataframe.
    """
    df_list = tabula.read_pdf(fpath, pages="all", multiple_tables=True, java_options=["-XX:ActiveProcessorCount=1"])

    # debug
    # split_doc = fitz.open(fpath)
    # if len(df_list) != len(split_doc):
    #     raise RuntimeError("error parsing table")
    # split_doc.close()

    merged = pd.concat(df_list, ignore_index=True)
    return merged


if __name__ == "__main__":
    main()
