from analyzer.base import Analyzer
from db import run_scalar
from utils import print_table

class PredicateAnalyzer(Analyzer):

  def analyze(self):
    if not self.qp.where_predicates:
      print("No WHERE predicates to analyze.")
      return
    if self.qp.has_or_in_where:
      print("WHERE contains OR; skipping predicate analysis for now.")
      return

    print("WHERE predicates:")
    for i, p in enumerate(self.qp.where_predicates, 1):
      print(f"  [{i}] {p}")

    base_count = self._count_with_where(self.qp.where_predicates)
    print(f"\nRows with all predicates: {base_count}")

    rows = []
    for i, p in enumerate(self.qp.where_predicates):
      without_i = [q for j, q in enumerate(self.qp.where_predicates) if j != i]
      cnt = self._count_with_where(without_i)
      rows.append((i + 1, p, cnt, cnt - base_count))

    rows.sort(key=lambda r: r[3], reverse=True)

    print("\nPredicate impact (relax one predicate at a time):")
    print_table(
      ["idx", "predicate", "rows_without_it", "extra_rows_vs_all"],
      rows,
      max_rows=len(rows),
      title="Most restrictive predicates"
    )

  def _count_with_where(self, predicates):
    base = self.qp.from_sql
    if predicates:
      where_sql = " AND ".join(predicates)
      base += f" WHERE {where_sql}"
    sql = f"SELECT COUNT(*) {base};"
    return run_scalar(sql)
