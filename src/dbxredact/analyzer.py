"""Presidio analyzer engine setup for PHI/PII detection."""

import spacy.util
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider


class SpacyModelNotFoundError(Exception):
    """Raised when required spaCy models are not installed."""
    pass


TEN_DIGIT_PHONE_PATTERN = Pattern(
    name="ten_digit_phone_pattern",
    regex=r"\b\d{10}\b",
    score=0.8,
)

WHITESPACE_PHONE_PATTERN = Pattern(
    name="whitespace_phone_pattern",
    regex=r"\(\s*\d{3}\s*\)\s*-\s*\d{3}\s*-\s*\d{4}",
    score=0.9,
)

PhoneRecognizer = PatternRecognizer(
    supported_entity="PHONE_NUMBER",
    patterns=[TEN_DIGIT_PHONE_PATTERN, WHITESPACE_PHONE_PATTERN],
    context=["phone", "call", "contact", "mobile"],
)

AGE_GENDER_PATTERN = Pattern(
    name="age_gender_pattern", regex=r"\b\d{1,3}\s?[YyMmFf]\b", score=0.8
)

AgeGenderRecognizer = PatternRecognizer(
    supported_entity="AGE_GENDER",
    patterns=[AGE_GENDER_PATTERN],
    context=["age", "sex", "gender"],
)


# Map language codes to open-source spaCy models (MIT licensed)
LANG_MODEL_MAP = {
    "en": "en_core_web_sm",
    "es": "es_core_news_sm",
}


def check_spacy_models(languages: list) -> tuple:
    """Check which spaCy models are installed.
    
    Args:
        languages: List of language codes to check
        
    Returns:
        Tuple of (available_languages, missing_models)
    """
    available = []
    missing = []
    
    for lang in languages:
        model_name = LANG_MODEL_MAP.get(lang)
        if model_name and spacy.util.is_package(model_name):
            available.append(lang)
        elif model_name:
            missing.append(model_name)
    
    return available, missing


def add_recognizers_to_analyzer(analyzer_engine):
    """Add custom recognizers to the analyzer engine."""
    analyzer_engine.registry.add_recognizer(PhoneRecognizer)
    analyzer_engine.registry.add_recognizer(AgeGenderRecognizer)
    return analyzer_engine


def get_analyzer_engine(
    add_pci: bool = True,
    add_phi: bool = True,
    languages: list = None,
    **kwargs
) -> AnalyzerEngine:
    """Initialize Presidio AnalyzerEngine with open-source spaCy models.
    
    Args:
        add_pci: Add PCI (payment card) recognizers
        add_phi: Add PHI (health information) recognizers  
        languages: List of language codes to support (default: ["en"])
        **kwargs: Additional arguments passed to AnalyzerEngine
    
    Returns:
        Configured AnalyzerEngine instance
        
    Raises:
        SpacyModelNotFoundError: If required spaCy models are not installed
    
    Supported languages and models (all MIT licensed):
        - en: en_core_web_sm
        - es: es_core_news_sm
    """
    if languages is None:
        languages = ["en"]
    
    # Validate language codes
    unsupported = [lang for lang in languages if lang not in LANG_MODEL_MAP]
    if unsupported:
        raise ValueError(f"Unsupported languages: {unsupported}. Supported: {list(LANG_MODEL_MAP.keys())}")
    
    # Check if models are installed (without triggering download)
    available_langs, missing_models = check_spacy_models(languages)
    
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
    
    # Build model config for available languages
    models = [
        {"lang_code": lang, "model_name": LANG_MODEL_MAP[lang]}
        for lang in available_langs
    ]
    
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
    
    if add_pci:
        pci_patterns = [
            Pattern(
                name="credit_card_basic", regex=r"\b(?:\d[ -]*?){13,19}\b", score=0.5
            ),
            Pattern(
                name="credit_card_grouped",
                regex=r"\b(?:\d{4}[ -]?){3}\d{4}\b",
                score=0.6,
            ),
            Pattern(
                name="iban",
                regex=r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b",
                score=0.8,
            ),
            Pattern(
                name="swift", regex=r"\b[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?\b", score=0.8
            ),
        ]
        pci_recognizer = PatternRecognizer(
            supported_entity="CREDIT_CARD",
            patterns=pci_patterns,
            context=[
                "credit",
                "card",
                "visa",
                "mastercard",
                "amex",
                "discover",
                "payment",
                "cvv",
                "cvc",
                "expiry",
                "expiration",
                "cardholder",
                "pan",
                "primary account",
            ],
        )
        analyzer.registry.add_recognizer(pci_recognizer)

    if add_phi:
        phi_patterns = [
            Pattern(name="mrn", regex=r"\bMRN[:\s]*\d{6,10}\b", score=0.8),
            Pattern(
                name="patient_id",
                regex=r"\b(?:PT|PAT|PATIENT)[:\s-]*\d{6,10}\b",
                score=0.8,
            ),
            Pattern(name="health_insurance", regex=r"\b[A-Z]{3}\d{9,12}\b", score=0.7),
            Pattern(
                name="medical_license",
                regex=r"\b(?:MD|DO|NP|RN)[:\s-]*\d{6,10}\b",
                score=0.7,
            ),
        ]
        phi_recognizer = PatternRecognizer(
            supported_entity="MEDICAL_RECORD_NUMBER",
            patterns=phi_patterns,
            context=[
                "mrn",
                "medical",
                "record",
                "patient",
                "health",
                "insurance",
                "doctor",
            ],
        )
        analyzer.registry.add_recognizer(phi_recognizer)

    analyzer = add_recognizers_to_analyzer(analyzer)
    return analyzer
