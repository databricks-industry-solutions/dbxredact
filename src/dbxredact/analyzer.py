"""Presidio analyzer engine setup for PHI/PII detection."""

import spacy.util
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider, NlpEngine, NlpArtifacts


class SpacyModelNotFoundError(Exception):
    """Raised when required spaCy models are not installed."""
    pass


REFERENCE_NUMBER_REGEX = r"\b(?!ICD|CPT|DSM|HER|COVID|SARS)[A-Z]{2,4}-\d[\d-]{3,}\b"


class _StubNlpEngine(NlpEngine):
    """No-op NLP engine for pattern-only Presidio (skips spaCy entirely)."""

    def __init__(self):
        self.nlp = {}

    def process_text(self, text, language):
        return NlpArtifacts(
            entities=[], tokens=[], lemmas=[], tokens_indices=[],
            nlp_engine=self, language=language,
        )

    def process_batch(self, texts, language, **kwargs):
        return [(t, self.process_text(t, language)) for t in texts]

    def is_loaded(self):
        return True

    def load(self):
        pass

    def get_supported_languages(self):
        return ["en"]

    def get_supported_entities(self):
        return []

    def is_stopword(self, word, language):
        return False

    def is_punct(self, word, language):
        return False


# Custom recognizer for age/gender patterns common in clinical text (e.g. "65F", "32M", "72Y")
# Restricted to 10-159 to avoid false positives on "5M" (5 million), "3M" (company), etc.
AGE_GENDER_PATTERN = Pattern(
    name="age_gender_pattern", regex=r"\b(?:1[0-5]\d|[1-9]\d)\s?[YFMyfm]\b", score=0.8
)

AgeGenderRecognizer = PatternRecognizer(
    supported_entity="AGE_GENDER",
    patterns=[AGE_GENDER_PATTERN],
    context=["age", "sex", "gender"],
)

# DD/MM/YY and DD/MM/YYYY date patterns (not covered by Presidio built-ins)
_DATE_DMY_PATTERNS = [
    Pattern(name="dd_mm_yyyy", regex=r"\b\d{1,2}/\d{1,2}/\d{4}\b", score=0.7),
    Pattern(name="dd_mm_yy", regex=r"\b\d{1,2}/\d{1,2}/\d{2}\b", score=0.6),
    Pattern(name="dd-mm-yyyy", regex=r"\b\d{1,2}-\d{1,2}-\d{4}\b", score=0.7),
    Pattern(name="dd-mm-yy", regex=r"\b\d{1,2}-\d{1,2}-\d{2}\b", score=0.6),
]

DateDMYRecognizer = PatternRecognizer(
    supported_entity="DATE_TIME",
    patterns=_DATE_DMY_PATTERNS,
    context=["date", "dob", "born", "admission", "discharge", "appointment"],
)

AgeYearsOldRecognizer = PatternRecognizer(
    supported_entity="AGE",
    patterns=[Pattern(
        name="age_years_old",
        regex=r"\b\d{1,3}[\s-]?year[\s-]?(?:s[\s-]?)?old\b",
        score=0.9,
    )],
)

# ---------------------------------------------------------------------------
# HIPAA Safe Harbor recognizers -- high-precision patterns for identifiers
# that have zero regex coverage from Presidio built-ins.
# ---------------------------------------------------------------------------

DeaNumberRecognizer = PatternRecognizer(
    supported_entity="DEA_NUMBER",
    patterns=[Pattern(name="dea_number", regex=r"\b[ABFGMRabfgmr][A-Za-z]\d{7}\b", score=0.9)],
)

NpiRecognizer = PatternRecognizer(
    supported_entity="NPI_NUMBER",
    patterns=[Pattern(name="npi_labeled", regex=r"\bNPI[:\s#]*\d{10}\b", score=0.9)],
    context=["npi", "provider", "national provider"],
)

DobLabeledRecognizer = PatternRecognizer(
    supported_entity="DATE_OF_BIRTH",
    patterns=[Pattern(
        name="dob_labeled",
        regex=r"\b(?:DOB|D\.O\.B\.?)[:\s]*\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b",
        score=0.95,
    )],
    context=["dob", "date of birth", "born"],
)

FaxNumberRecognizer = PatternRecognizer(
    supported_entity="FAX_NUMBER",
    patterns=[Pattern(
        name="fax_labeled",
        regex=r"\b(?:FAX|Fax)[:\s#.]*\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
        score=0.9,
    )],
    context=["fax"],
)

HealthPlanIdRecognizer = PatternRecognizer(
    supported_entity="HEALTH_PLAN_ID",
    patterns=[Pattern(
        name="health_plan_id",
        regex=r"(?i)\b(?:MEMBER|POLICY|SUBSCRIBER|GROUP|BENEFICIARY|PLAN)\s*(?:ID|#|NO\.?|NUMBER|:)[:\s#-]*[A-Z0-9]{4,15}\b",
        score=0.8,
    )],
    context=["member", "policy", "subscriber", "beneficiary", "plan", "insurance"],
)

AccountNumberRecognizer = PatternRecognizer(
    supported_entity="ACCOUNT_NUMBER",
    patterns=[Pattern(
        name="account_number",
        regex=r"(?i)\b(?:ACCT|ACCOUNT)\s*(?:NO\.?|#|NUMBER|:)[:\s#-]*\d{6,17}\b",
        score=0.8,
    )],
    context=["account", "acct", "billing"],
)

VinRecognizer = PatternRecognizer(
    supported_entity="VIN",
    patterns=[Pattern(name="vin_labeled", regex=r"\bVIN[:\s#]*[A-HJ-NPR-Z0-9]{17}\b", score=0.9)],
    context=["vin", "vehicle"],
)

MacAddressRecognizer = PatternRecognizer(
    supported_entity="DEVICE_ID",
    patterns=[Pattern(
        name="mac_address",
        regex=r"\b(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}\b",
        score=0.9,
    )],
)

EinRecognizer = PatternRecognizer(
    supported_entity="US_EIN",
    patterns=[Pattern(
        name="ein_labeled",
        regex=r"(?i)\b(?:EIN|TAX\s*ID|EMPLOYER\s*ID)[:\s#]*\d{2}-\d{7}\b",
        score=0.9,
    )],
    context=["ein", "tax", "employer"],
)

SsnNoDashRecognizer = PatternRecognizer(
    supported_entity="US_SSN",
    patterns=[Pattern(
        name="ssn_no_dash",
        regex=r"(?i)\b(?:SSN|SOCIAL\s*SECURITY(?:\s*(?:NO\.?|NUMBER|#))?)[:\s#]*\d{9}\b",
        score=0.85,
    )],
    context=["ssn", "social security"],
)

AgeOver89Recognizer = PatternRecognizer(
    supported_entity="AGE",
    patterns=[Pattern(
        name="age_over_89",
        regex=r"(?i)\b(?:AGE|AGED?)[:\s]*(?:9[0-9]|1[0-4]\d|150)\b",
        score=0.9,
    )],
    context=["age", "years"],
)

LicenseNumberRecognizer = PatternRecognizer(
    supported_entity="LICENSE_NUMBER",
    patterns=[
        Pattern(
            name="license_cert_label",
            regex=r"(?i)\b(?:LICENSE|LIC|CERT(?:IFICATE)?)\s*(?:NO\.?|#|NUMBER)[:\s#]*[A-Z0-9]{5,12}\b",
            score=0.75,
        ),
        Pattern(
            name="license_cert_digit",
            regex=r"(?i)\b(?:LICENSE|LIC|CERT(?:IFICATE)?)[:\s#]+\d[A-Z0-9]{4,11}\b",
            score=0.75,
        ),
    ],
    context=["license", "certificate", "certification"],
)

MbiRecognizer = PatternRecognizer(
    supported_entity="HEALTH_PLAN_ID",
    patterns=[Pattern(
        name="mbi",
        regex=r"\b[1-9][AC-HJ-KM-NP-RT-Y][AC-HJ-KM-NP-RT-Y0-9]\d[AC-HJ-KM-NP-RT-Y][AC-HJ-KM-NP-RT-Y0-9]\d[A-Z]{2}\d{2}\b",
        score=0.9,
    )],
    context=["medicare", "mbi", "beneficiary"],
)

PassportRecognizer = PatternRecognizer(
    supported_entity="ID_NUMBER",
    patterns=[Pattern(
        name="passport_labeled",
        regex=r"(?i)\bPASSPORT[:\s#]+(?=[A-Z0-9]*\d)[A-Z0-9]{6,9}\b",
        score=0.9,
    )],
    context=["passport", "travel"],
)

ZipLabeledRecognizer = PatternRecognizer(
    supported_entity="LOCATION",
    patterns=[Pattern(
        name="zip_labeled",
        regex=r"(?i)\bZIP(?:\s*CODE)?[:\s#]*\d{5}(?:-\d{4})?\b",
        score=0.85,
    )],
    context=["zip", "postal", "address"],
)

RoutingRecognizer = PatternRecognizer(
    supported_entity="ACCOUNT_NUMBER",
    patterns=[Pattern(
        name="routing_labeled",
        regex=r"(?i)\b(?:ROUTING|ABA|RTN)[:\s#]*[0-3]\d{8}\b",
        score=0.85,
    )],
    context=["routing", "aba", "bank", "wire"],
)

ItinRecognizer = PatternRecognizer(
    supported_entity="US_SSN",
    patterns=[Pattern(
        name="itin_labeled",
        regex=r"(?i)\bITIN[:\s#]*9\d{2}-?\d{2}-?\d{4}\b",
        score=0.9,
    )],
    context=["itin", "taxpayer"],
)

_HIPAA_SAFE_HARBOR_RECOGNIZERS = [
    DeaNumberRecognizer, NpiRecognizer, DobLabeledRecognizer,
    FaxNumberRecognizer, HealthPlanIdRecognizer, AccountNumberRecognizer,
    VinRecognizer, MacAddressRecognizer, EinRecognizer,
    SsnNoDashRecognizer, AgeOver89Recognizer, LicenseNumberRecognizer,
    MbiRecognizer, PassportRecognizer, ZipLabeledRecognizer,
    RoutingRecognizer, ItinRecognizer,
]


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
    ref_recognizer = PatternRecognizer(
        supported_entity="ID_NUMBER",
        patterns=[Pattern(name="reference_number", regex=REFERENCE_NUMBER_REGEX, score=0.6)],
        context=["reference", "case", "claim", "application", "wire", "dispute"],
    )
    analyzer.registry.add_recognizer(ref_recognizer)

    analyzer.registry.add_recognizer(DateDMYRecognizer)
    analyzer.registry.add_recognizer(AgeYearsOldRecognizer)
    for rec in _HIPAA_SAFE_HARBOR_RECOGNIZERS:
        analyzer.registry.add_recognizer(rec)
    analyzer = add_recognizers_to_analyzer(analyzer)
    return analyzer


def get_pattern_only_analyzer(
    default_score_threshold: float = 0.5,
    **kwargs,
) -> AnalyzerEngine:
    """AnalyzerEngine using only pattern recognizers -- no spaCy required.

    Loads Presidio's built-in pattern recognizers (SSN, phone, email, credit card,
    IP, etc.) plus custom MRN, reference-ID, age/gender, and DD/MM date recognizers.
    NER-based recognizers still load but return nothing because the NLP engine is a
    no-op stub.
    """
    analyzer = AnalyzerEngine(
        nlp_engine=_StubNlpEngine(),
        supported_languages=["en"],
        default_score_threshold=default_score_threshold,
        **kwargs,
    )

    # Custom healthcare patterns
    phi_patterns = [
        Pattern(name="mrn", regex=r"\bMRN[:\s]*\d{6,10}\b", score=0.8),
        Pattern(name="patient_id", regex=r"\b(?:PT|PAT|PATIENT)[:\s-]*\d{6,10}\b", score=0.8),
    ]
    analyzer.registry.add_recognizer(PatternRecognizer(
        supported_entity="MEDICAL_RECORD_NUMBER",
        patterns=phi_patterns,
        context=["mrn", "medical", "record", "patient"],
    ))

    # Business reference / case IDs
    analyzer.registry.add_recognizer(PatternRecognizer(
        supported_entity="ID_NUMBER",
        patterns=[Pattern(name="reference_number", regex=REFERENCE_NUMBER_REGEX, score=0.6)],
        context=["reference", "case", "claim", "application", "wire", "dispute"],
    ))

    analyzer.registry.add_recognizer(AgeGenderRecognizer)
    analyzer.registry.add_recognizer(DateDMYRecognizer)
    analyzer.registry.add_recognizer(AgeYearsOldRecognizer)
    for rec in _HIPAA_SAFE_HARBOR_RECOGNIZERS:
        analyzer.registry.add_recognizer(rec)
    return analyzer
