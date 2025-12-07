from tabulate import tabulate

def print_table(colnames, rows, title=None, max_rows=20):
    if title:
        print(f"\n=== {title} ===")
    if not rows:
        print("(no rows)")
        return
    if len(rows) > max_rows:
        rows = rows[:max_rows]
        print(f"(showing first {max_rows} rows)")
    print(tabulate(rows, headers=colnames, tablefmt="psql"))
