# analyzer.py
from db import run_query, run_scalar

def analyze_sum_contributions(sum_query: str,
                              contrib_expr: str,
                              from_clause: str,
                              key_cols=("order_id",)):
    """
    1. Run the original SUM query and get the total.
    2. Run a 'breakdown' query that computes each row's contribution.
    3. Return (total, breakdown_rows, colnames).
    """
    # 1. Run original query
    total = run_scalar(sum_query)

    # 2. Build per-row contribution query
    #    SELECT <keys>, <contrib_expr> AS contrib, *
    key_select = ", ".join(key_cols)
    breakdown_sql = f"""
        SELECT {key_select}, {contrib_expr} AS contrib
        FROM {from_clause}
        ORDER BY contrib DESC;
    """

    colnames, rows = run_query(breakdown_sql)
    return total, colnames, rows


def find_unmatched_keys(left_table: str,
                        right_table: str,
                        join_col: str):
    """
    Find values of join_col in left_table that have no match in right_table
    and vice versa.
    """
    # left values that don't match right
    left_sql = f"""
        SELECT {join_col}, COUNT(*) AS cnt
        FROM {left_table} l
        WHERE NOT EXISTS (
            SELECT 1 FROM {right_table} r
            WHERE r.{join_col} = l.{join_col}
        )
        GROUP BY {join_col}
        ORDER BY cnt DESC;
    """
    left_cols, left_rows = run_query(left_sql)

    # right values that don't match left
    right_sql = f"""
        SELECT {join_col}, COUNT(*) AS cnt
        FROM {right_table} r
        WHERE NOT EXISTS (
            SELECT 1 FROM {left_table} l
            WHERE l.{join_col} = r.{join_col}
        )
        GROUP BY {join_col}
        ORDER BY cnt DESC;
    """
    right_cols, right_rows = run_query(right_sql)

    return (left_cols, left_rows), (right_cols, right_rows)
