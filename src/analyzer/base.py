from abc import ABC, abstractmethod
from sqlmeta import QueryProfile


class Analyzer(ABC):

  def __init__(self, qp: QueryProfile, verbose = False):
    if not isinstance(qp, QueryProfile):
      raise TypeError("Analyzer requires a QueryProfile instance")
    self.qp = qp
    self.verbose = verbose

  @abstractmethod
  def analyze(self):
    pass

  def has_aggregate(self) -> bool:
    return len(self.qp.aggregates) > 0

  def is_grouped(self) -> bool:
    return len(self.qp.group_by) > 0

  def has_where(self) -> bool:
    return bool(self.qp.where_predicates)

  def has_joins(self) -> bool:
    return len(self.qp.joins) > 0

  def print_header(self, title: str):
    print("\n" + "=" * 50)
    print(title)
    print("=" * 50 + "\n")

  def explain(self, message: str):
    print("• " + message)
  
  def show_table(self, cols, rows, title):
    if self.verbose:
        print_table(cols, rows, title=title)
