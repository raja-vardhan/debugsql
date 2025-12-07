from dataclasses import dataclass, field
from typing import List, Tuple, Optional
import sqlglot
from sqlglot import exp


@dataclass
class JoinCondition:
  left_alias: str
  left_col: str
  right_alias: str
  right_col: str


@dataclass
class QueryProfile:
  sql: str
  expected: Optional[str] = None
  select_exprs: List[str] = field(default_factory=list)
  aggregates: List[Tuple[str, str]] = field(default_factory=list) 
  tables: dict = field(default_factory=dict)                      
  joins: List[JoinCondition] = field(default_factory=list)
  where_predicates: List[str] = field(default_factory=list)
  has_or_in_where: bool = False
  group_by: List[str] = field(default_factory=list)

  from_sql: Optional[str] = None
  where_sql: Optional[str] = None

  def __post_init__(self):
    self._parse()

  def _parse(self):
    tree = sqlglot.parse_one(self.sql)

    self._extract_select(tree)
    self._extract_from_and_joins(tree)
    self._extract_where(tree)
    self._extract_group_by(tree)

  def _extract_select(self, tree: exp.Expression):
    select = tree.find(exp.Select)
    if not select:
      return

    for e in select.expressions:
      self.select_exprs.append(e.sql())
      agg = e.find(exp.AggFunc)
      if agg:
        func_name = agg.__class__.__name__.upper()
        arg_expr = agg.args.get("this")
        expr_str = arg_expr.sql() if arg_expr is not None else "*"
        self.aggregates.append((func_name, expr_str))

  
  def _reconstruct_from_sql(self, from_expr: exp.From):
    parts = []

    base_table = from_expr.this
    parts.append("FROM " + base_table.sql())

    select_node = from_expr.parent
    for j in select_node.find_all(exp.Join):
        parts.append(j.sql())

    return " ".join(parts)


  def _extract_from_and_joins(self, tree: exp.Expression):
    select = tree.find(exp.Select)
    if not select:
      return

    from_expr = select.find(exp.From)
    if not from_expr:
      return

    self.from_sql = self._reconstruct_from_sql(from_expr)

    for tbl in select.find_all(exp.Table):
        table_name = tbl.name
        alias_node = tbl.args.get("alias")
        if isinstance(alias_node, exp.TableAlias):
            alias = alias_node.name
        else:
            alias = table_name
        self.tables[alias] = table_name

    for join_node in select.find_all(exp.Join):
      on_expr = join_node.args.get("on")
      if not on_expr:
        continue

      for cond in self._split_conjuncts(on_expr):
        if not isinstance(cond, exp.EQ):
          continue

        left, right = cond.left, cond.right
        if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
          continue
        if not left.table or not right.table:
          continue

        self.joins.append(
          JoinCondition(
            left_alias=left.table,
            left_col=left.name,
            right_alias=right.table,
            right_col=right.name,
          )
        )

  def _extract_where(self, tree: exp.Expression):
    select = tree.find(exp.Select)
    if not select:
      return
    where = select.args.get("where")
    if not where:
      return

    self.where_sql = where.this.sql()

    for cond in self._split_conjuncts(where.this):
      if isinstance(cond, exp.Or):
        self.has_or_in_where = True
      self.where_predicates.append(cond.sql())

  def _extract_group_by(self, tree: exp.Expression):
    select = tree.find(exp.Select)
    if not select:
      return
    group = select.args.get("group")
    if not group:
      return

    for e in group.expressions:
      self.group_by.append(e.sql())

  def _split_conjuncts(self, expr: exp.Expression):
    if isinstance(expr, exp.And):
      return self._split_conjuncts(expr.left) + self._split_conjuncts(expr.right)
    return [expr]
