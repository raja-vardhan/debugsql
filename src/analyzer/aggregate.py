from analyzer.base import Analyzer
from db import run_scalar, run_query
from utils import print_table

class AggregateAnalyzer(Analyzer):

  def analyze(self):
    print("Detected aggregate query.")
    print("Aggregates:", self.qp.aggregates)

    cols, rows = run_query(self.qp.sql)
    print_table(cols, rows, title="Original Aggregate Result")

    for func, expr in self.qp.aggregates:
      if func == "SUM":
        self._analyze_sum(expr)
      elif func == "COUNT":
        self._analyze_count(expr)
      elif func == "AVG":
        self._analyze_avg(expr)

  def _build_base_from_where(self):
    clause = self.qp.from_sql
    if self.qp.where_sql:
      clause += f" WHERE {self.qp.where_sql}"
    return clause

  def _analyze_sum(self, expr: str):
    print("\n[SUM] Contribution analysis")

    from_where = self._build_base_from_where()

    if self.qp.group_by:
      group_cols = ", ".join(self.qp.group_by)
      sql = f"""
        SELECT {group_cols}, SUM({expr}) AS contrib
        {from_where}
        GROUP BY {group_cols}
        ORDER BY contrib DESC
        LIMIT 50;
      """
    else:
      sql = f"""
        SELECT *, {expr} AS contrib
        {from_where}
        ORDER BY contrib DESC
        LIMIT 50;
      """

    cols, rows = run_query(sql)
    print_table(cols, rows, title="Top contributions to SUM")

  def _analyze_count(self, expr: str):
    print("\n[COUNT] Contribution analysis (groups / rows)")

    from_where = self._build_base_from_where()

    if self.qp.group_by:
      group_cols = ", ".join(self.qp.group_by)
      sql = f"""
        SELECT {group_cols}, COUNT({expr}) AS contrib
        {from_where}
        GROUP BY {group_cols}
        ORDER BY contrib DESC
        LIMIT 50;
      """
    else:
      sql = f"""
        SELECT *, 1 AS contrib
        {from_where}
        LIMIT 50;
      """

    cols, rows = run_query(sql)
    print_table(cols, rows, title="Groups/rows contributing to COUNT")

  def _analyze_avg(self, expr: str):
    print("\n[AVG] Decomposed as SUM/COUNT")

    from_where = self._build_base_from_where()

    if self.qp.group_by:
      group_cols = ", ".join(self.qp.group_by)
      sql = f"""
        SELECT
          {group_cols},
          COUNT({expr}) AS cnt,
          SUM({expr}) AS total,
          AVG({expr}) AS avg_val
        {from_where}
        GROUP BY {group_cols}
        ORDER BY avg_val DESC
        LIMIT 50;
      """
    else:
      sql = f"""
        SELECT
          COUNT({expr}) AS cnt,
          SUM({expr}) AS total,
          AVG({expr}) AS avg_val
        {from_where};
      """

    cols, rows = run_query(sql)
    print_table(cols, rows, title="AVG decomposition (count/total/avg)")
