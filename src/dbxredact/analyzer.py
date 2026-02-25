"""Presidio analyzer engine setup for PHI/PII detection."""

import spacy.util
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider


class SpacyModelNotFoundError(Exception):
    """Raised when required spaCy models are not installed."""
    pass


# Custom recognizer for age/gender patterns common in clinical text (e.g. "65F", "32M")
AGE_GENDER_PATTERN = Pattern(
    name="age_gender_pattern", regex=r"\b\d{1,3}\s?[YyMmFf]\b", score=0.8
)

AgeGenderRecognizer = PatternRecognizer(
    supported_entity="AGE_GENDER",
    patterns=[AGE_GENDER_PATTERN],
    context=["age", "sex", "gender"],
)


# Map language codes to open-source spaCy models (MIT licensed).
# NER F1 benchmarks (en): sm=84.6%, lg=85.4%, trf=90.2%.
# trf uses a RoBERTa backbone and benefits from GPU (available on ML runtime).
# Fallback order: trf -> lg -> md -> sm so the best available model is always used.
LANG_MODEL_MAP = {
    "en": {
        "sm": "en_core_web_sm",
        "md": "en_core_web_md",
        "lg": "en_core_web_lg",
        "trf": "en_core_web_trf",
    },
    "es": {
        "sm": "es_core_news_sm",
        "md": "es_core_news_md",
        "lg": "es_core_news_lg",
    },
}

DEFAULT_MODEL_SIZE = "trf"


def _resolve_model_name(lang: str, model_size: str) -> str:
    """Resolve a (lang, size) pair to a spaCy model name."""
    lang_models = LANG_MODEL_MAP.get(lang, {})
    return lang_models.get(model_size, lang_models.get("sm", ""))


def check_spacy_models(languages: list, model_size: str = None) -> tuple:
    """Check which spaCy models are installed.

    Args:
        languages: List of language codes to check
        model_size: One of 'sm', 'md', 'lg'. Falls back to smaller models if unavailable.

    Returns:
        Tuple of (list of (lang, resolved_model_name), list of missing_models)
    """
    if model_size is None:
        model_size = DEFAULT_MODEL_SIZE

    available = []
    missing = []

    fallback_order = {
        "trf": ["trf", "lg", "md", "sm"],
        "lg": ["lg", "md", "sm"],
        "md": ["md", "sm"],
        "sm": ["sm"],
    }
    sizes_to_try = fallback_order.get(model_size, ["sm"])

    for lang in languages:
        found = False
        for size in sizes_to_try:
            name = _resolve_model_name(lang, size)
            if name and spacy.util.is_package(name):
                available.append((lang, name))
                found = True
                break
        if not found:
            preferred = _resolve_model_name(lang, model_size)
            if preferred:
                missing.append(preferred)

    return available, missing


def add_recognizers_to_analyzer(analyzer_engine):
    """Add custom recognizers to the analyzer engine."""
    analyzer_engine.registry.add_recognizer(AgeGenderRecognizer)
    return analyzer_engine


def get_analyzer_engine(
    add_pci: bool = True,
    add_phi: bool = True,
    languages: list = None,
    model_size: str = None,
    **kwargs
) -> AnalyzerEngine:
    """Initialize Presidio AnalyzerEngine with open-source spaCy models.

    Args:
        add_pci: Add PCI (payment card) recognizers
        add_phi: Add PHI (health information) recognizers
        languages: List of language codes to support (default: ["en"])
        model_size: spaCy model size -- 'sm', 'md', or 'lg' (default: 'lg').
            Falls back to smaller models if the requested size is not installed.
        **kwargs: Additional arguments passed to AnalyzerEngine

    Returns:
        Configured AnalyzerEngine instance

    Raises:
        SpacyModelNotFoundError: If required spaCy models are not installed

    Supported languages and models (all MIT licensed):
        - en: en_core_web_{sm,md,lg}
        - es: es_core_news_{sm,md,lg}
    """
    if languages is None:
        languages = ["en"]
    if model_size is None:
        model_size = DEFAULT_MODEL_SIZE

    unsupported = [lang for lang in languages if lang not in LANG_MODEL_MAP]
    if unsupported:
        raise ValueError(f"Unsupported languages: {unsupported}. Supported: {list(LANG_MODEL_MAP.keys())}")

    available_pairs, missing_models = check_spacy_models(languages, model_size)

    if missing_models:
        install_instructions = "\n".join([
            f"  %pip install https://github.com/explosion/spacy-models/releases/download/{m}-3.8.0/{m}-3.8.0-py3-none-any.whl"
            for m in missing_models
        ])
        raise SpacyModelNotFoundError(
            f"Required spaCy models not installed: {missing_models}\n\n"
            f"Install them in your notebook or cluster init script:\n{install_instructions}\n\n"
            f"Or disable Presidio detection and use AI Query only."
        )

    models = [
        {"lang_code": lang, "model_name": model_name}
        for lang, model_name in available_pairs
    ]
    available_langs = [lang for lang, _ in available_pairs]
    
    # Configure NLP engine with pre-installed spaCy models
    nlp_config = {
        "nlp_engine_name": "spacy",
        "models": models
    }
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()
    
    analyzer = AnalyzerEngine(
        nlp_engine=nlp_engine,
        supported_languages=available_langs,
        **kwargs
    )
    
    # Presidio's built-in CREDIT_CARD recognizer (with Luhn checksum) is used by
    # default -- no custom PCI patterns needed.

    if add_phi:
        # Only add patterns that aren't covered by Presidio built-ins.
        # MRN and Patient ID patterns are unique to healthcare and not in Presidio.
        phi_patterns = [
            Pattern(name="mrn", regex=r"\bMRN[:\s]*\d{6,10}\b", score=0.8),
            Pattern(
                name="patient_id",
                regex=r"\b(?:PT|PAT|PATIENT)[:\s-]*\d{6,10}\b",
                score=0.8,
            ),
        ]
        phi_recognizer = PatternRecognizer(
            supported_entity="MEDICAL_RECORD_NUMBER",
            patterns=phi_patterns,
            context=["mrn", "medical", "record", "patient"],
        )
        analyzer.registry.add_recognizer(phi_recognizer)

    # Business reference / case IDs (AP-2024-09-3382, WIRE-2024-081590, etc.)
    ref_patterns = [
        Pattern(
            name="reference_number",
            regex=r"\b[A-Z]{2,4}-[\d-]{4,}\b",
            score=0.6,
        ),
    ]
    ref_recognizer = PatternRecognizer(
        supported_entity="ID_NUMBER",
        patterns=ref_patterns,
        context=["reference", "case", "claim", "application", "wire", "dispute"],
    )
    analyzer.registry.add_recognizer(ref_recognizer)

    analyzer = add_recognizers_to_analyzer(analyzer)
    return analyzer
