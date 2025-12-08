from analyzer.base import Analyzer
from db import run_query, run_scalar
from utils import print_table

from analyzer.minsubset import greedy_minimal_subset, count_metric


class JoinAnalyzer(Analyzer):

    def __init__(self, qp, expected_count: float | None = None):
        super().__init__(qp)
        self.expected_count = expected_count

    def analyze(self):
        print("\n[Join Analysis] Are rows missing or exploding due to join mismatches?")

        if not self.qp.joins:
            self.explain("No joins detected.")
            return

        for j in self.qp.joins:
            self.explain(
                f"Join condition: {j.left_alias}.{j.left_col} = "
                f"{j.right_alias}.{j.right_col}"
            )
            self._check_mismatches(j)

        self._fanout_analysis()

        if self.expected_count is not None:
            self._minimal_explosion_explanation()

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
    
    def _get_primary_key_columns(self, table_name: str):
      sql = """
          SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
          JOIN pg_class c ON c.oid = i.indrelid
          WHERE c.relname = %s AND i.indisprimary = TRUE
          ORDER BY a.attnum;
      """
      colnames, rows = run_query(sql, params=(table_name,))

      return [r[0] for r in rows] if rows else []

    def _fanout_analysis(self):

      if not self.qp.tables:
          return

      print("\n[Fan-out Analysis] Contribution of each table to join multiplicity")

      from_where = self.qp.from_sql
      if self.qp.where_sql:
          from_where += f" WHERE {self.qp.where_sql}"

      for alias, table_name in self.qp.tables.items():

          pk_cols = self._get_primary_key_columns(table_name)

          if not pk_cols:
              print(f"\nWarning: Table '{table_name}' has no primary key. "
                    "Using first column as a surrogate key.")
              pk_cols = [self.get_table_columns(table_name)[0]]

          fq_pk_cols = [f"{alias}.{col}" for col in pk_cols]
          pk_select = ", ".join(fq_pk_cols)

          fanout_sql = f"""
              SELECT {pk_select}, COUNT(*) AS multiplicity
              {from_where}
              GROUP BY {pk_select}
              ORDER BY multiplicity DESC
              LIMIT 10;
          """

          print(f"\nFan-out for table '{table_name}' (alias '{alias}') using PK {pk_cols}:")
          cols_out, rows = run_query(fanout_sql)
          print_table(cols_out, rows, title=f"Top fan-out rows for {table_name}")


    def _minimal_explosion_explanation(self):
        expected = self.expected_count

        from_where = f"FROM {self.qp.from_sql}"
        if self.qp.where_sql:
            from_where += f" WHERE {self.qp.where_sql}"

        count_sql = f"SELECT COUNT(*) {from_where};"
        _, count_rows = run_query(count_sql)
        actual_count = count_rows[0][0] if count_rows else 0

        print(f"\n[Join Explosion] Expected rows ≈ {expected}, actual rows = {actual_count}")

        if actual_count <= expected:
            self.explain(
                "Join result size is not greater than the expected count; "
                "no explosion to explain."
            )
            return

        full_sql = f"SELECT * {from_where};"
        cols, rows = run_query(full_sql)

        if not rows:
            self.explain("No joined rows found (unexpected, given the COUNT).")
            return

        metric_fn = count_metric()

        explanation_subset = greedy_minimal_subset(
            rows,
            metric_fn=metric_fn,
            expected_value=float(expected),
            direction=">",
        )

        print("\n=== Minimal Explanation Subset for Join Explosion ===")
        print(
            f"These {len(explanation_subset)} joined row(s) are sufficient that, "
            "if removed, the join result would no longer exceed the expected count."
        )

        print_table(cols, explanation_subset, title="Minimal Explanation Subset (Joined Rows)")
