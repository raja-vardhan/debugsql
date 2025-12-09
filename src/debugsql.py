import argparse
import utils
import time
from sqlmeta import QueryProfile
from analyzer.aggregate import AggregateAnalyzer
from analyzer.join import JoinAnalyzer
from analyzer.predicates import PredicateAnalyzer
from analyzer.nonagg import NonAggregateAnalyzer
from analyzer.expected import ExpectedResultAnalyzer
from analyzer.why_not import WhyNotAnalyzer
from db import run_query

def report_time(t_start):
    elapsed = (time.perf_counter() - t_start) * 1000
    print(f"\n[DebugSQL] Execution time: {elapsed:.2f} ms\n")

def main():

    parser = argparse.ArgumentParser(description="DebugSQL")
    subparsers = parser.add_subparsers(dest="cmd")

    # Common args
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--query", required=True, help="SQL query to debug")

    # Aggregate subparser
    agg_parser = subparsers.add_parser("agg", parents=[common], help="Analyze aggregate contributions")
    agg_parser.add_argument("--expected-sum", type=float)
    agg_parser.add_argument("--expected-count", type=float)
    agg_parser.add_argument("--expected-avg", type=float)

    # Join subparser
    join_parser = subparsers.add_parser("join", parents=[common], help="Analyze join key mismatches")
    join_parser.add_argument("--expected-count", type=float)

    # Predicate analysis subparser
    predicate_parser = subparsers.add_parser("predicate", parents=[common], help="Analyze predicates")


    # Why-not parser
    why_not_parser = subparsers.add_parser("why-not", parents=[common], help="Explain why a tuple is missing")
    why_not_parser.add_argument("--table", required=True, help="Base table name (e.g., Movie)")
    why_not_parser.add_argument("--key", required=True, help="Key predicate, e.g., primary_title = 'Inception'")
    why_not_parser.add_argument(
        "--output",
        choices=["summary", "detailed", "both"],
        default="summary",
        help="Output mode"
    )


    args = parser.parse_args()

    t0 = time.perf_counter()

    if args.cmd == "agg":
        qp = QueryProfile(args.query)
        analyzer = AggregateAnalyzer(
            qp,
            expected_sum=args.expected_sum,
            expected_count=args.expected_count,
            expected_avg=args.expected_avg)
        print("\n==============================")
        print(f"Running {analyzer.__class__.__name__}")
        print("==============================\n")
        analyzer.analyze()
    elif args.cmd == "join":
        qp = QueryProfile(args.query)
        analyzer = JoinAnalyzer(qp, expected_count=args.expected_count)
        print("\n==============================")
        print(f"Running {analyzer.__class__.__name__}")
        print("==============================\n")
        analyzer.analyze()
    elif args.cmd == "predicate":
        qp = QueryProfile(args.query)
        analyzer = PredicateAnalyzer(qp)
        if qp.where_predicates and not qp.has_or_in_where:
            analyzer.analyze()
        else:
            print("No predicates")
    elif args.cmd == "why-not":
        qp = QueryProfile(args.query)
        analyzer = WhyNotAnalyzer(
            qp,
            table=args.table,
            key_predicate=args.key,
            output=args.output,
            verbose=False
        )
        print("\n==============================")
        print(f"Running {analyzer.__class__.__name__}")
        print("==============================\n")
        analyzer.analyze()
    else:
        cols, rows = run_query(args.query)
        utils.print_table(cols, rows)

    report_time(t0)

if __name__ == "__main__":
    main()
