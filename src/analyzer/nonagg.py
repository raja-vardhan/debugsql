from analyzer.base import Analyzer
from db import run_query
from utils import print_table

class NonAggregateAnalyzer(Analyzer):
  def analyze(self):
    print("Non-aggregate SELECT query.")
    cols, rows = run_query(self.qp.sql)
    print_table(cols, rows, title="Query Result", max_rows=50)
