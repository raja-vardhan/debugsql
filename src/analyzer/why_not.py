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
        alias_to_preds = {}
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

    def _analyze_why_not(self) -> Explanation:
        bullets = []
        details = {}

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
        bullets.append("The base tuple exists.")

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
        conjuncts = self._extract_where_conjuncts(where_expr.this) if where_expr is not None else []
        failing_base = []

        for conj in conjuncts:
            if self._expr_uses_only_alias(conj, base_alias):
                pred_sql = conj.sql(dialect="postgres")
                ok = self._eval_predicate_on_base(base_alias, pred_sql)
                if ok is False:
                    failing_base.append(pred_sql)

        if failing_base:
            bullets.append("Base-table predicate failures:")
            for p in failing_base:
                bullets.append(f"  • `{p}` is FALSE for this tuple.")
            details["failing_base_predicates"] = failing_base
        else:
            bullets.append("All base-table predicates are satisfied.")

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

        return Explanation(
            title=f"Why is tuple `{self.key_predicate}` missing?",
            bullets=bullets,
            details=details,
        )
    
    def _strip_alias_from_key_predicate(self, key_predicate: str, base_alias: str) -> str:
        if key_predicate.strip().startswith(base_alias + "."):
            return key_predicate.split(".", 1)[1]
        return key_predicate

