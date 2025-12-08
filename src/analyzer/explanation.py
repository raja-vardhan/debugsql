from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class Explanation:
  title: str
  bullets: List[str]
  details: Optional[Dict[str, Any]] = None

  def render_explanation(self, mode: str = "summary") -> None:
    if mode in ("summary", "both"):
      print(self.title)
      print()
      for b in self.bullets:
          print(f"- {b}")
      print()

    if mode in ("detailed", "both") and self.details:
      for name, table in self.details.items():
        print(f"=== {name} ===")
        print(table)
        print()