import os

"""
Cấu hình chung cho đường dẫn dự án, giúp các script preprocessor dùng thống nhất.

Giả định cây thư mục:
  PROJECT_ROOT/
    data/
      raw/
      processed/
    src/
      preprocessor/
        *.py
"""

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DATA_RAW = os.path.join(DATA_DIR, "raw")
DATA_PROCESSED = os.path.join(DATA_DIR, "processed")


