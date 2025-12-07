import argparse
from sqlmeta import QueryProfile
from analyzer.aggregate import AggregateAnalyzer
from analyzer.join import JoinAnalyzer
from analyzer.predicates import PredicateAnalyzer
from analyzer.nonagg import NonAggregateAnalyzer

def select_analyzers(qp: QueryProfile, verbose = False):
    analyzers = []

    if qp.aggregates:
        analyzers.append(AggregateAnalyzer(qp, verbose=verbose))

    if qp.joins:
        analyzers.append(JoinAnalyzer(qp, verbose=verbose))

    if qp.where_predicates and not qp.has_or_in_where:
        analyzers.append(PredicateAnalyzer(qp, verbose=verbose))

    if not analyzers:
        analyzers.append(NonAggregateAnalyzer(qp, verbose=verbose))

    return analyzers

def main():
    parser = argparse.ArgumentParser(description="DebugSQL")
    parser.add_argument("--query", required=True, help="SQL query to debug")
    parser.add_argument("--expected", type=str, help="User-expected result (value or comma-separated list)")
    parser.add_argument("--verbose", action="store_true", help="Show detailed tables and raw diagnostics instead of summaries")
    args = parser.parse_args()

    qp = QueryProfile(args.query, expected=args.expected)
    analyzers = select_analyzers(qp, verbose=args.verbose)

    for a in analyzers:
        print("\n==============================")
        print(f"Running {a.__class__.__name__}")
        print("==============================\n")
        a.analyze()

if __name__ == "__main__":
    main()
