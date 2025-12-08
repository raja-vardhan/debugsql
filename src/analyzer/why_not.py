from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from analyzer.base import Analyzer
from db import run_query
import sqlglot
from sqlglot import exp
from analyzer.explanation import Explanation


class WhyNotAnalyzer(Analyzer):

    def __init__(self, qp, table: str, key_predicate: str, output: str, verbose=False):
        super().__init__(qp, verbose=verbose)
        self.qp = qp
        self.table = table
        self.key_predicate = key_predicate
        self.output = output

    def analyze(self):
        expl = self._analyze_why_not()
        expl.render_explanation(mode=self.output)


    def _get_base_row(self, pred: str):
        sql = f"""
            SELECT *
            FROM {self.table}
            WHERE {pred}
        """
        return run_query(sql)

    def _parse_query(self):
        return sqlglot.parse_one(self.qp.sql, read="postgres")

    def _find_base_alias(self, parsed: exp.Select) -> Optional[str]:
        for t in parsed.find_all(exp.Table):
            if t.name.lower() == self.table.lower():
                return t.alias_or_name
        return None

    def _alias_to_table(self, alias: str) -> str:
        return self.qp.tables.get(alias, alias)

    def _extract_where_conjuncts(self, where_expr):
        if where_expr is None:
            return []

        def collect(e, acc):
            if isinstance(e, exp.And):
                collect(e.left, acc)
                collect(e.right, acc)
            else:
                acc.append(e)

        lst = []
        collect(where_expr, lst)
        return lst

    def _expr_uses_only_alias(self, expr_node, alias: str) -> bool:
        for col in expr_node.find_all(exp.Column):
            if col.table and col.table.lower() != alias.lower():
                return False
        return True

    def _eval_predicate_on_base(self, alias: str, predicate_sql: str) -> Optional[bool]:
        sql = f"""
            SELECT ({predicate_sql})::boolean AS ok
            FROM {self.table} AS {alias}
            WHERE {self.key_predicate}
        """
        _, rows = run_query(sql)
        if not rows:
            return None
        val = rows[0][0]
        return bool(val) if val is not None else False

    def _get_join_aliases(self, parsed: exp.Select) -> List[str]:
        aliases = []
        for j in parsed.find_all(exp.Join):
            table_expr = j.this
            aliases.append(table_expr.alias_or_name)
        return aliases

    def _group_where_by_alias(self, parsed: exp.Select) -> Dict[str, List[exp.Expression]]:
        alias_to_preds: Dict[str, List[exp.Expression]] = {}
        where_expr = parsed.args.get("where")
        if where_expr is None:
            return alias_to_preds

        conjuncts = self._extract_where_conjuncts(where_expr.this)

        for conj in conjuncts:
            cols = list(conj.find_all(exp.Column))
            if not cols:
                continue

            aliases = {c.table for c in cols if c.table}
            if len(aliases) == 1:
                alias = aliases.pop()
                alias_to_preds.setdefault(alias, []).append(conj)

        return alias_to_preds

    def _fetch_join_rows_for_alias(self, alias: str) -> Tuple[List[str], List[Tuple]]:
        fj_clause = self.qp.from_sql

        sql = f"""
            SELECT {alias}.*
            {fj_clause}
            WHERE {self.key_predicate}
        """

        try:
            return run_query(sql)
        except Exception as e:
            if self.verbose:
                print(f"[DEBUG] Cannot fetch rows for alias {alias}: {e}")
            return [], []

    def _check_join_predicate_failure(
        self, alias: str, predicate: exp.Expression, rows, colnames
    ):
        predicate_sql = predicate.sql(dialect="postgres")
        actual_values = []
        ok_any = False

        for r in rows:
            row_dict = dict(zip(colnames, r))
            actual_values.append(row_dict)

            projection = ", ".join(f"{repr(v)} AS {k}" for k, v in row_dict.items())

            chk_sql = f"""
                SELECT ({predicate_sql})::boolean AS ok
                FROM (SELECT {projection}) AS {alias}
            """

            _, chk_rows = run_query(chk_sql)
            if chk_rows and chk_rows[0][0] is True:
                ok_any = True
                break

        if ok_any:
            return None

        return {
            "alias": alias,
            "predicate": predicate_sql,
            "reason": f"No rows for alias `{alias}` satisfy predicate `{predicate_sql}`.",
            "actual_values": actual_values,
        }

    def _analyze_join_failures(self, parsed: exp.Select, base_alias: str):
        failures = []

        join_aliases = self._get_join_aliases(parsed)
        preds_by_alias = self._group_where_by_alias(parsed)

        for alias in join_aliases:
            colnames, rows = self._fetch_join_rows_for_alias(alias)

            if not rows:
                failures.append(
                    {
                        "alias": alias,
                        "predicate": None,
                        "reason": f"No joined rows exist for alias `{alias}` for this tuple.",
                        "actual_values": [],
                    }
                )
                continue

            for pred in preds_by_alias.get(alias, []):
                failure = self._check_join_predicate_failure(
                    alias, pred, rows, colnames
                )
                if failure:
                    failures.append(failure)

        return failures

    def _strip_alias_from_key_predicate(self, key_predicate: str, base_alias: str) -> str:
        if base_alias and key_predicate.strip().startswith(base_alias + "."):
            return key_predicate.split(".", 1)[1]
        return key_predicate

    def _compute_minimal_subset(self, failing_base, join_failures):
        causes = []

        for fb in failing_base:
            causes.append({
                "type": "predicate_failure",
                "predicate": fb["sql"],
                "expr": fb["expr"],
                "reason": f"Base-table predicate `{fb['sql']}` is FALSE for this tuple."
            })

        alias_groups: Dict[str, List[Dict[str, Any]]] = {}
        for jf in join_failures:
            alias_groups.setdefault(jf["alias"], []).append(jf)

        for alias, failures in alias_groups.items():
            if len(failures) == 1:
                jf = failures[0]
                causes.append({
                    "type": "join_failure",
                    "alias": alias,
                    "predicate": jf["predicate"],
                    "reason": jf["reason"],
                    "actual_values": jf.get("actual_values", []),
                })
            else:
                preds = [f["predicate"] for f in failures if f["predicate"]]
               
                causes.append({
                    "type": "join_failure_group",
                    "alias": alias,
                    "predicates": preds,
                    "reason": f"Alias `{alias}` has multiple blocking join conditions.",
                    "actual_values": [f.get("actual_values", []) for f in failures],
                })

        n = len(causes)
        if n > 0:
            for c in causes:
                c["responsibility"] = 1.0 / n

        return causes

    def _describe_predicate_repair(self, expr: exp.Expression, sql_str: str,
                                   base_row_dict: Dict[str, Any]) -> str:
        suggestion = (
            f"Relax or modify predicate `{sql_str}` so it becomes TRUE "
            "for the base tuple."
        )

        simple_ops = (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ)
        if not isinstance(expr, simple_ops):
            return suggestion

        left, right = expr.left, expr.right
        col_node = None
        lit_node = None
        flipped = False

        if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
            col_node = left
            lit_node = right
        elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
            col_node = right
            lit_node = left
            flipped = True

        if col_node is None or lit_node is None:
            return suggestion

        col_name = col_node.name
        literal_val = lit_node.this

        if col_name not in base_row_dict:
            return suggestion

        actual_val = base_row_dict[col_name]

        try:
            actual_num = float(actual_val)
            lit_num = float(literal_val)
        except Exception:
            return suggestion

        op = type(expr)

        if op is exp.GT and not flipped:
            suggestion += (
                f" For example, decrease the threshold from {lit_num} down to "
                f"{actual_num} or smaller, or increase `{col_name}` above {lit_num}."
            )
        elif op is exp.GTE and not flipped:
            suggestion += (
                f" For example, decrease the threshold from {lit_num} down to "
                f"{actual_num} or smaller, or increase `{col_name}` to at least {lit_num}."
            )
        elif op is exp.LT and not flipped:
            suggestion += (
                f" For example, increase the threshold from {lit_num} up to "
                f"{actual_num} or larger, or decrease `{col_name}` below {lit_num}."
            )
        elif op is exp.LTE and not flipped:
            suggestion += (
                f" For example, increase the threshold from {lit_num} up to "
                f"{actual_num} or larger, or decrease `{col_name}` to at most {lit_num}."
            )
        else:
            suggestion += (
                f" (Current value of `{col_name}` is {actual_val} vs literal {literal_val}.)"
            )

        return suggestion

    def _build_repair_suggestions(
        self,
        minimal_subset: List[Dict[str, Any]],
        base_row_info: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        
        suggestions = []

        base_row_dict: Dict[str, Any] = {}
        if base_row_info and "columns" in base_row_info and "row" in base_row_info:
            base_row_dict = dict(zip(base_row_info["columns"], base_row_info["row"]))

        for cause in minimal_subset:
            ctype = cause["type"]

            if ctype == "predicate_failure":
                expr = cause.get("expr")
                pred_sql = cause["predicate"]
                text = self._describe_predicate_repair(expr, pred_sql, base_row_dict)
                suggestions.append({
                    "type": "predicate_repair",
                    "predicate": pred_sql,
                    "cause": cause,
                    "suggestion": text,
                })

            elif ctype in ("join_failure", "join_failure_group"):
                alias = cause["alias"]
                table_name = self._alias_to_table(alias)
                preds = cause.get("predicates") or ([cause["predicate"]]
                                                    if cause.get("predicate") else [])
                preds_str = ", ".join(f"`{p}`" for p in preds if p)

                if cause.get("actual_values"):
                    text = (
                        f"Ensure there is at least one row in table `{table_name}` "
                        f"(alias `{alias}`) that matches the base tuple on join keys "
                        f"and satisfies all join-side predicates {preds_str or '(none)'}."
                    )
                else:
                    text = (
                        f"Insert or modify a row in table `{table_name}` (alias `{alias}`) "
                        "so that it joins with the base tuple (i.e., satisfies the join "
                        f"condition) and any relevant predicates {preds_str or '(none)'}."
                    )

                suggestions.append({
                    "type": "join_repair",
                    "alias": alias,
                    "table": table_name,
                    "cause": cause,
                    "suggestion": text,
                })

        return suggestions

    def _analyze_why_not(self) -> Explanation:
        bullets: List[str] = []
        details: Dict[str, Any] = {}

        parsed = self._parse_query()
        base_alias = self._find_base_alias(parsed)

        base_pred = self._strip_alias_from_key_predicate(self.key_predicate, base_alias)

        base_cols, base_rows = self._get_base_row(base_pred)

        if not base_rows:
            bullets.append(
                f"The tuple `{self.key_predicate}` does not exist in table `{self.table}`."
            )
            return Explanation(
                title=f"Why is tuple `{self.key_predicate}` missing?",
                bullets=bullets,
                details=None,
            )

        details["base_row"] = {"columns": base_cols, "row": base_rows[0]}
        bullets.append("The base tuple exists in the underlying table.")

        parsed = self._parse_query()
        base_alias = self._find_base_alias(parsed)

        if base_alias is None:
            bullets.append(
                f"The query does not reference table `{self.table}`, so the tuple cannot appear."
            )
            return Explanation(
                title=f"Why is tuple `{self.key_predicate}` missing?",
                bullets=bullets,
                details=details,
            )

        where_expr = parsed.args.get("where")
        conjuncts = (
            self._extract_where_conjuncts(where_expr.this)
            if where_expr is not None
            else []
        )
        failing_base: List[Dict[str, Any]] = []

        for conj in conjuncts:
            if self._expr_uses_only_alias(conj, base_alias):
                pred_sql = conj.sql(dialect="postgres")
                ok = self._eval_predicate_on_base(base_alias, pred_sql)
                if ok is False:
                    failing_base.append({"sql": pred_sql, "expr": conj})

        if failing_base:
            bullets.append("Base-table predicate failures:")
            for fb in failing_base:
                bullets.append(f"  • `{fb['sql']}` is FALSE for this tuple.")
            details["failing_base_predicates"] = [fb["sql"] for fb in failing_base]
        else:
            bullets.append("All base-table predicates on the base tuple are satisfied.")

        join_failures = self._analyze_join_failures(parsed, base_alias)

        if join_failures:
            bullets.append("Join-based failures:")
            for jf in join_failures:
                if jf["predicate"]:
                    bullets.append(
                        f"  • Alias `{jf['alias']}` has no rows satisfying `{jf['predicate']}`."
                    )
                else:
                    bullets.append(
                        f"  • Alias `{jf['alias']}` has no matching joined rows."
                    )
            details["join_failures"] = join_failures
        else:
            bullets.append(
                "No join-based failures detected. If the tuple is still absent, grouping or HAVING may be responsible."
            )

        minimal_subset = self._compute_minimal_subset(failing_base, join_failures)
        details["minimal_subset"] = minimal_subset

        if minimal_subset:
            bullets.append("Minimal subset of causes preventing this tuple from appearing:")
            for m in minimal_subset:
                if m["type"] == "predicate_failure":
                    bullets.append(
                        f"  • Base-table predicate `{m['predicate']}` fails "
                        f"(responsibility ≈ {m['responsibility']:.2f})."
                    )
                elif m["type"] == "join_failure":
                    bullets.append(
                        f"  • Join alias `{m['alias']}` with blocking predicate "
                        f"`{m['predicate']}` (responsibility ≈ {m['responsibility']:.2f})."
                    )
                elif m["type"] == "join_failure_group":
                    preds = ", ".join(m["predicates"]) if m["predicates"] else "(no predicates)"
                    bullets.append(
                        f"  • Join alias `{m['alias']}` with multiple blocking "
                        f"conditions: {preds} (responsibility ≈ {m['responsibility']:.2f})."
                    )
        else:
            bullets.append(
                "No explicit blocking predicates or joins were found; the tuple may be excluded by grouping, HAVING, or DISTINCT."
            )

        repair_suggestions = self._build_repair_suggestions(
            minimal_subset, details.get("base_row")
        )
        details["repair_suggestions"] = repair_suggestions

        if repair_suggestions:
            bullets.append("Possible repairs (hypothetical changes that would allow the tuple to appear):")
            for r in repair_suggestions:
                bullets.append(f"  • {r['suggestion']}")

        return Explanation(
            title=f"Why is tuple `{self.key_predicate}` missing?",
            bullets=bullets,
            details=details,
        )
