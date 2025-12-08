from analyzer.base import Analyzer
from db import run_scalar, run_query
from utils import print_table

from analyzer.minsubset import (
    greedy_minimal_subset,
    sum_metric_from_contrib_index,
    count_metric,
    avg_metric_from_value_index,
)


class AggregateAnalyzer(Analyzer):

    def __init__(
        self,
        qp,
        expected_sum: float | None = None,
        expected_count: float | None = None,
        expected_avg: float | None = None,
    ):
        super().__init__(qp)
        self.expected_sum = expected_sum
        self.expected_count = expected_count
        self.expected_avg = expected_avg

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

        if self.expected_sum is None:
            return

        actual_sum = run_scalar(self.qp.sql)
        expected = self.expected_sum

        print(f"\nExpected SUM = {expected}, Actual SUM = {actual_sum}")

        if actual_sum > expected:
            contrib_idx = len(cols) - 1
            metric_fn = sum_metric_from_contrib_index(contrib_idx)

            explanation_subset = greedy_minimal_subset(
                rows,
                metric_fn=metric_fn,
                expected_value=expected,
                direction=">",
            )

            print("\n=== Minimal Explanation Subset for High SUM ===")
            print(
                f"These {len(explanation_subset)} row(s) are sufficient that,"
                " if removed, SUM would no longer exceed the expected value."
            )
            print_table(cols, explanation_subset, title="Minimal Explanation Subset")

        elif actual_sum < expected:
            print(
                "Actual SUM is LOWER than expected. "
                "Explaining this requires analyzing *missing* tuples "
                "(joins/predicates/why-not), not just present contributors.\n"
                "Use predicate/join/why-not analyzers for a deeper explanation."
            )
        else:
            print("Actual SUM equals expected; no surprise to explain.")

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

        if self.expected_count is None:
            return

        actual_count = run_scalar(self.qp.sql)
        expected = self.expected_count

        print(f"\nExpected COUNT = {expected}, Actual COUNT = {actual_count}")

        if actual_count > expected:
            if self.qp.group_by:
                contrib_idx = len(cols) - 1
                metric_fn = sum_metric_from_contrib_index(contrib_idx)
            else:
                metric_fn = count_metric()

            explanation_subset = greedy_minimal_subset(
                rows,
                metric_fn=metric_fn,
                expected_value=expected,
                direction=">",
            )

            print("\n=== Minimal Explanation Subset for High COUNT ===")
            print(
                f"These {len(explanation_subset)} row(s)/group(s) are sufficient that,"
                " if removed, COUNT would no longer exceed the expected value."
            )
            print_table(cols, explanation_subset, title="Minimal Explanation Subset")
        elif actual_count < expected:
            print(
                "Actual COUNT is LOWER than expected. "
                "Explaining this needs reasoning about tuples that never appear "
                "in the result (missing joins / filtered rows). "
                "Use predicate/join/why-not analyses for that."
            )
        else:
            print("Actual COUNT equals expected; no surprise to explain.")

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

        if self.expected_avg is None or self.qp.group_by:
            if self.expected_avg is not None and self.qp.group_by:
                print(
                    "\nExpected AVG provided, but AVG is grouped. "
                    "Per-group expected averages are not supported yet for minimal subsets."
                )
            return

        actual_avg = run_scalar(self.qp.sql)
        expected = self.expected_avg

        print(f"\nExpected AVG = {expected}, Actual AVG = {actual_avg}")

        if actual_avg == expected:
            print("Actual AVG equals expected; no surprise to explain.")
            return

        per_row_sql = f"""
            SELECT {expr} AS val
            {from_where};
        """
        val_cols, val_rows = run_query(per_row_sql)

        if not val_rows:
            print("No rows available to explain AVG at row level.")
            return

        val_index = 0
        metric_fn = avg_metric_from_value_index(val_index)

        direction = ">" if actual_avg > expected else "<"

        explanation_subset = greedy_minimal_subset(
            val_rows,
            metric_fn=metric_fn,
            expected_value=expected,
            direction=direction,
        )

        label = "High AVG" if direction == ">" else "Low AVG"
        print(f"\n=== Minimal Explanation Subset for {label} ===")
        print(
            f"These {len(explanation_subset)} row(s) are sufficient that, "
            "if removed, the AVG would cross the expected value."
        )
        print_table(val_cols, explanation_subset, title="Minimal Explanation Subset (rows by value)")
