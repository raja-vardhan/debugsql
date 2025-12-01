# debugsql.py
import argparse

from db import run_query, run_scalar
from analyzer import analyze_sum_contributions, find_unmatched_keys
from utils import print_table

def main():
    parser = argparse.ArgumentParser(description="DebugSQL: Explain query results via data subsets")
    parser.add_argument("--query", required=True,
                        help="The SQL query to run (e.g., aggregate query).")
    parser.add_argument("--expected", type=float,
                        help="Optional expected numeric result (for comparison).")
    parser.add_argument("--contrib-expr",
                        help="Expression used in SUM() for contribution analysis (e.g. 'amount * multiplier').")
    parser.add_argument("--from-clause",
                        help="FROM/JOIN clause for contribution analysis.")
    parser.add_argument("--sum-mode", action="store_true",
                        help="If set, treat query as SUM() query and run contribution analysis.")
    parser.add_argument("--check-joins", action="store_true",
                        help="If set, run join mismatch detection for Sales/ExchangeRates on 'region'.")
    args = parser.parse_args()

    # Run the main query
    print("Running main query...")
    try:
        # If it's a simple scalar (SUM), we can use run_scalar
        if args.sum_mode:
            result = run_scalar(args.query)
            print(f"\nResult: {result}")
        else:
            cols, rows = run_query(args.query)
            print_table(cols, rows, title="Query Result")
    except Exception as e:
        print("Error running main query:", e)
        return

    # Compare with expected, if given
    if args.expected is not None and args.sum_mode:
        diff = result - args.expected
        print(f"Expected: {args.expected}  |  Actual: {result}  |  Diff: {diff}")

    # Contribution analysis
    if args.sum_mode and args.contrib_expr and args.from_clause:
        print("\nPerforming contribution analysis...")
        total, colnames, rows = analyze_sum_contributions(
            sum_query=args.query,
            contrib_expr=args.contrib_expr,
            from_clause=args.from_clause,
            key_cols=("order_id",)
        )
        print(f"\nTotal from contribution query: {total}")
        print_table(colnames, rows, title="Per-row contributions (sorted by contrib)", max_rows=20)

    # Join mismatch detection
    if args.check_joins:
        print("\nChecking join mismatches between Sales.region and ExchangeRates.region...")
        (lcols, lrows), (rcols, rrows) = find_unmatched_keys(
            left_table="Sales",
            right_table="ExchangeRates",
            join_col="region"
        )
        print_table(lcols, lrows, title="Regions in Sales with no match in ExchangeRates")
        print_table(rcols, rrows, title="Regions in ExchangeRates with no match in Sales")


if __name__ == "__main__":
    main()
