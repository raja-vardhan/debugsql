from analyzer.base import Analyzer
from db import run_query
from utils import print_table

class JoinAnalyzer(Analyzer):

  def analyze(self):
    print("\n[Join Analysis] Are rows missing due to join mismatches?")

    if not self.qp.joins:
      self.explain("No joins detected.")
      return

    for j in self.qp.joins:
      self.explain(f"Join condition: {j.left_alias}.{j.left_col} = {j.right_alias}.{j.right_col}")
     
      self._check_mismatches(j)

    

  def _alias_to_table(self, alias: str) -> str:
    return self.qp.tables.get(alias, alias)

  def _check_mismatches(self, j):
    lt = self._alias_to_table(j.left_alias)
    rt = self._alias_to_table(j.right_alias)

    print(f"\n[Join mismatch] {lt}.{j.left_col} <-> {rt}.{j.right_col}")

    left_sql = f"""
      SELECT {j.left_col}, COUNT(*) AS cnt
      FROM {lt} AS {j.left_alias}
      WHERE NOT EXISTS (
        SELECT 1 FROM {rt} AS {j.right_alias}
        WHERE {j.right_alias}.{j.right_col} = {j.left_alias}.{j.left_col}
      )
      GROUP BY {j.left_col}
      ORDER BY cnt DESC
      LIMIT 20;
    """

    right_sql = f"""
      SELECT {j.right_col}, COUNT(*) AS cnt
      FROM {rt} AS {j.right_alias}
      WHERE NOT EXISTS (
        SELECT 1 FROM {lt} AS {j.left_alias}
        WHERE {j.left_alias}.{j.left_col} = {j.right_alias}.{j.right_col}
      )
      GROUP BY {j.right_col}
      ORDER BY cnt DESC
      LIMIT 20;
    """

    lcols, lrows = run_query(left_sql)
    rcols, rrows = run_query(right_sql)

    print_table(lcols, lrows, title=f"{lt} keys with no match in {rt}")
    print_table(rcols, rrows, title=f"{rt} keys with no match in {lt}")

  def get_table_columns(self, table_name: str):
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """
    colnames, rows = run_query(sql, params=(table_name,))
    return [r[0] for r in rows]

  def _fanout_analysis(self):
    if not self.qp.tables:
      return

    print("\n[Fan-out] Approximate row multiplicity per base table")

    from_where = f"FROM {self.qp.from_sql}"
    if self.qp.where_sql:
      from_where += f" WHERE {self.qp.where_sql}"

    for alias, table_name in self.qp.tables.items():
      cols = self.get_table_columns(table_name)
      if not cols:
        continue

      group_exprs = ", ".join(f"{alias}.{c}" for c in cols)

      sql = f"""
        SELECT {group_exprs}, COUNT(*) AS multiplicity
        {from_where}
        GROUP BY {group_exprs}
        ORDER BY multiplicity DESC
        LIMIT 5;
      """

      print(f"\nFan-out for base table {table_name} (alias {alias}):")
      cols_out, rows = run_query(sql)
      print_table(cols_out, rows, title=f"Top fan-out rows for {table_name}")
