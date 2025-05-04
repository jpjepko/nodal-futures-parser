import math
import os
from concurrent.futures import ProcessPoolExecutor

import tabula
import fitz # from pymupdf
import pandas as pd


# globals
NUM_CORES = 16
FILE_NAME = "EOD_FUTURES_REPORT.PDF"
SPLIT_DIR = "split"


def main():
    print(f"splitting into {NUM_CORES} files...")
    split_pdf(FILE_NAME, NUM_CORES)

    print(f"parsing on {NUM_CORES} cores...")
    split_paths = sorted([os.path.join(SPLIT_DIR, f) for f in os.listdir(SPLIT_DIR) if f.endswith(".pdf")])
    #parse_pdf(f"./{SPLIT_DIR}/01.pdf")
    with ProcessPoolExecutor() as executor:
        res = list(executor.map(parse_pdf, split_paths))
    
    print("merging...")
    merged = pd.concat(res, ignore_index=True)
    
    # cleanup headers
    merged.columns = [col.replace('\r', ' ').replace('*', '') for col in merged.columns]

    # either use dataframe or save as csv
    print(merged)
    #merged.to_csv("merged.csv", index=False)


def split_pdf(fname: str, num_files: int, out_dir: str = SPLIT_DIR):
    """Split PDF fname into num_files. Outputs in out_dir.

    Args:
        fname (str): File name.
        num_files (int): How many split files there should be.
        out_dir (str, optional): Where the split files go. Defaults to "split".
    """
    doc = fitz.open(fname)
    pages_per_split = math.ceil(len(doc) / num_files)

    # add zero paddings so files can be sorted correctly
    padding = len(str(NUM_CORES - 1))
    os.mkdir(out_dir)

    for i in range(num_files):
        split_doc = fitz.open()
        begin_page = i * pages_per_split

        split_doc.insert_pdf(doc, from_page=begin_page, to_page=min(begin_page + pages_per_split - 1, len(doc) - 1))

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
    df_list = tabula.read_pdf(fpath, pages="all", multiple_tables=True)
    
    # debug
    # split_doc = fitz.open(fpath)
    # if len(df_list) != len(split_doc):
    #     raise RuntimeError("error parsing table")
    # split_doc.close()

    merged = pd.concat(df_list, ignore_index=True)
    return merged


if __name__ == "__main__":
    main()
