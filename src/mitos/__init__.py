import warnings

try:
    from ibis.backends.databricks import Backend as DatabricksBackend

    _orig_post_connect = DatabricksBackend._post_connect

    def _post_connect_best_effort(self, *, memtable_volume: str) -> None:
        try:
            _orig_post_connect(self, memtable_volume=memtable_volume)
        except Exception as e:
            warnings.warn(
                "Mitos: Databricks memtable volume creation failed; continuing without it. "
                "If you use `ibis.memtable` (directly or indirectly), execution may fail unless "
                "a writable UC volume exists at the expected location. "
                f"Original error: {e}"
            )

    DatabricksBackend._post_connect = _post_connect_best_effort

except ImportError:
    pass
except Exception as e:
    warnings.warn(f"Mitos: Failed to patch Databricks backend: {e}")

from .cohort_expression import (
    CohortExpression,
    PrimaryCriteria,
    ResultLimit,
    ObservationFilter,
)
from .concept_set import ConceptSet, ConceptSetExpression, ConceptSetItem
from .criteria import (
    Criteria,
    CriteriaGroup,
    Concept,
    ConceptSetSelection,
    NumericRange,
    DateRange,
    TextFilter,
)
from .tables import ConditionOccurrence, ConditionEra, VisitOccurrence, DrugExposure
from .build_context import BuildContext, CohortBuildOptions, compile_codesets

__all__ = [
    "CohortExpression",
    "PrimaryCriteria",
    "ResultLimit",
    "ObservationFilter",
    "ConceptSet",
    "ConceptSetExpression",
    "ConceptSetItem",
    "Criteria",
    "CriteriaGroup",
    "Concept",
    "ConceptSetSelection",
    "NumericRange",
    "DateRange",
    "TextFilter",
    "ConditionOccurrence",
    "ConditionEra",
    "VisitOccurrence",
    "DrugExposure",
    "BuildContext",
    "CohortBuildOptions",
    "compile_codesets",
]
