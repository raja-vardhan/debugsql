from typing import Callable, List, Any

Row = Any

def greedy_minimal_subset(
    rows: List[Row],
    metric_fn: Callable[[List[Row]], float],
    expected_value: float,
    direction: str = ">"
) -> List[Row]:
    
    if not rows:
        return []

    baseline = metric_fn(rows)

    if direction == ">":
        surprising = baseline > expected_value
        still_surprising = lambda v: v > expected_value
    elif direction == "<":
        surprising = baseline < expected_value
        still_surprising = lambda v: v < expected_value
    else:
        raise ValueError(f"Unknown direction: {direction}")

    if not surprising:
        return []

    impacts = []
    for r in rows:
        remaining = [x for x in rows if x is not r]
        new_val = metric_fn(remaining)
        impact = abs(baseline - new_val)
        impacts.append((impact, r))

    impacts.sort(key=lambda p: p[0], reverse=True)

    explanation = []
    remaining_rows = list(rows)

    for impact, r in impacts:
        explanation.append(r)
        remaining_rows.remove(r)
        new_val = metric_fn(remaining_rows)
        if not still_surprising(new_val):
            break

    return explanation


def sum_metric_from_contrib_index(contrib_index: int) -> Callable[[List[Row]], float]:
    def metric(rows: List[Row]) -> float:
        return sum(float(r[contrib_index]) for r in rows)
    return metric


def count_metric() -> Callable[[List[Row]], float]:
    return lambda rows: float(len(rows))


def avg_metric_from_value_index(val_index: int) -> Callable[[List[Row]], float]:
    def metric(rows: List[Row]) -> float:
        if not rows:
            return 0.0
        total = sum(float(r[val_index]) for r in rows)
        return total / len(rows)
    return metric
