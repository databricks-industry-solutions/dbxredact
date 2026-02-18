"""
dbxredact - PII/PHI Detection and Redaction Library

This library provides tools for detecting, evaluating, and redacting Protected Health
Information (PHI) and Personally Identifiable Information (PII) in text data.

Main components:
- Presidio-based detection
- AI/LLM-based detection
- GLiNER NER detection
- Entity alignment between different detection methods
- Evaluation and metrics
- Text redaction
"""

from .config import (
    PRESIDIO_ENTITY_TYPES,
    LABEL_ENUMS,
    PHI_PROMPT_SKELETON,
    ENTITY_TYPES_TO_IGNORE,
    ENTITY_TEXT_IGNORE_PATTERNS,
    should_ignore_entity,
    GLINER_LABEL_MAP,
    DEFAULT_GLINER_THRESHOLD,
    JUDGE_PROMPT_SKELETON,
    NEXT_ACTION_PROMPT_SKELETON,
    PROMPT_VERSION,
)

from .utils import (
    is_fuzzy_match,
    is_overlap,
    calculate_overlap,
    calculate_string_overlap,
)

from .analyzer import SpacyModelNotFoundError

from .presidio import (
    format_presidio_batch_results,
    make_presidio_batch_udf,
)

from .ai_detector import (
    make_prompt,
    format_entity_response_object_udf,
)

from .alignment import (
    align_entities_row,
    align_entities_udf,
)

from .evaluation import (
    evaluate_detection,
    calculate_metrics,
    format_contingency_table,
    format_metrics_summary,
    save_evaluation_results,
    compare_methods_across_datasets,
    get_best_method_per_dataset,
    analyze_errors,
    build_recall_matrix,
    summarize_method_strengths,
)

from .detection import (
    run_presidio_detection,
    run_ai_query_detection,
    run_gliner_detection,
    run_detection,
)

from .redaction import (
    redact_text,
    create_redaction_udf,
    create_redacted_table,
    apply_redaction_to_columns,
    RedactionStrategy,
)

from .metadata import (
    get_columns_by_tag,
    get_protected_columns,
    get_table_metadata,
)

from .pipeline import (
    run_detection_pipeline,
    run_redaction_pipeline,
    run_redaction_pipeline_streaming,
    run_redaction_pipeline_by_tag,
    OutputStrategy,
    AlignmentMode,
)

from .judge import (
    run_judge_evaluation,
    compute_judge_summary,
    run_next_action_query,
)

__all__ = [
    # Config
    "PRESIDIO_ENTITY_TYPES",
    "LABEL_ENUMS",
    "PHI_PROMPT_SKELETON",
    "ENTITY_TYPES_TO_IGNORE",
    "ENTITY_TEXT_IGNORE_PATTERNS",
    "should_ignore_entity",
    "JUDGE_PROMPT_SKELETON",
    "NEXT_ACTION_PROMPT_SKELETON",
    "PROMPT_VERSION",
    # Utils
    "is_fuzzy_match",
    "is_overlap",
    "calculate_overlap",
    "calculate_string_overlap",
    # Errors
    "SpacyModelNotFoundError",
    # Presidio
    "format_presidio_batch_results",
    "make_presidio_batch_udf",
    # AI Detection
    "make_prompt",
    "format_entity_response_object_udf",
    # Alignment
    "align_entities_row",
    "align_entities_udf",
    # Evaluation
    "evaluate_detection",
    "calculate_metrics",
    "format_contingency_table",
    "format_metrics_summary",
    "save_evaluation_results",
    "compare_methods_across_datasets",
    "get_best_method_per_dataset",
    "analyze_errors",
    "build_recall_matrix",
    "summarize_method_strengths",
    # Detection
    "run_presidio_detection",
    "run_ai_query_detection",
    "run_gliner_detection",
    "run_detection",
    # Redaction
    "redact_text",
    "create_redaction_udf",
    "create_redacted_table",
    "apply_redaction_to_columns",
    "RedactionStrategy",
    # Metadata
    "get_columns_by_tag",
    "get_protected_columns",
    "get_table_metadata",
    # Pipeline
    "run_detection_pipeline",
    "run_redaction_pipeline",
    "run_redaction_pipeline_streaming",
    "run_redaction_pipeline_by_tag",
    "OutputStrategy",
    "AlignmentMode",
    # Judge
    "run_judge_evaluation",
    "compute_judge_summary",
    "run_next_action_query",
]

