"""Configuration constants for PHI/PII detection."""

import hashlib as _hashlib
import logging as _logging
from dataclasses import dataclass, field
from typing import Optional, Any

_logger = _logging.getLogger(__name__)

# Governance floor thresholds -- prevent configs that silently disable detection
MIN_SCORE_THRESHOLD = 0.1
MIN_GLINER_THRESHOLD = 0.05


def _entity_schema():
    """Shared Spark schema for entity arrays returned by all detector UDFs.

    Defined as a function to avoid importing pyspark at config-load time
    (which would break lightweight test environments that mock pyspark).
    """
    from pyspark.sql.types import (
        ArrayType, StructType, StructField, StringType, IntegerType, DoubleType,
    )
    return ArrayType(StructType([
        StructField("entity", StringType()),
        StructField("entity_type", StringType()),
        StructField("score", DoubleType()),
        StructField("start", IntegerType()),
        StructField("end", IntegerType()),
        StructField("doc_id", StringType()),
    ]))

# Presidio entity types to detect.  These use Presidio's actual recognizer names.
# Pass to analyze_dict(entities=...) to restrict detection to this set.
PRESIDIO_ENTITY_TYPES = [
    "PERSON",
    "PHONE_NUMBER",
    "EMAIL_ADDRESS",
    "US_SSN",
    "LOCATION",
    "DATE_TIME",
    "IP_ADDRESS",
    "URL",
    "CREDIT_CARD",
    "IBAN_CODE",
    "US_DRIVER_LICENSE",
    "MEDICAL_RECORD_NUMBER",
    "AGE_GENDER",
    "AGE",
    "NRP",
    # HIPAA Safe Harbor recognizers
    "DEA_NUMBER",
    "NPI_NUMBER",
    "DATE_OF_BIRTH",
    "FAX_NUMBER",
    "HEALTH_PLAN_ID",
    "ACCOUNT_NUMBER",
    "VIN",
    "DEVICE_ID",
    "US_EIN",
    "LICENSE_NUMBER",
]

# Extended list of label enums for AI-based detection
LABEL_ENUMS = [
    "PERSON",
    "PHONE_NUMBER",
    "NRP",
    "UK_NHS",
    "AU_ACN",
    "LOCATION",
    "DATE_TIME",
    "AU_MEDICARE",
    "MEDICAL_RECORD_NUMBER",
    "AU_TFN",
    "EMAIL_ADDRESS",
    "US_SSN",
    "VIN",
    "IP",
    "DRIVER_LICENSE",
    "BIRTH_DATE",
    "APPOINTMENT_DATE_TIME",
    "HOSPITAL_NAME",
    "ORGANIZATION",
    "ID_NUMBER",
]

# Prompt template for AI-based PHI detection
PHI_PROMPT_SKELETON = """
You are an expert in Protected Health Information (PHI) detection who will help identify all PHI entities in a piece of medical text.

Qualifying PHI includes:
1. Names;
2. All geographical subdivisions smaller than a State, including street address, city, county, precinct, zip code, and their equivalent geocodes, except for the initial three digits of a zip code, if according to the current publicly available data from the Bureau of the Census: (1) The geographic unit formed by combining all zip codes with the same three initial digits contains more than 20,000 people; and (2) The initial three digits of a zip code for all such geographic units containing 20,000 or fewer people is changed to 000.
3. All elements of dates (except year) for dates directly related to an individual, including birth date, admission date, discharge date, date of death; and all ages over 89 and all elements of dates (including year) indicative of such age, except that such ages and elements may be aggregated into a single category of age 90 or older;
4. Phone numbers;
5. Fax numbers;
6. Electronic mail addresses;
7. Social Security numbers;
8. Medical record numbers;
9. Health plan beneficiary numbers;
10. Account numbers;
11. Certificate/license numbers;
12. Vehicle identifiers and serial numbers, including license plate numbers;
13. Device identifiers and serial numbers;
14. Web Universal Resource Locators (URLs);
15. Internet Protocol (IP) address numbers;
16. Biometric identifiers, including finger and voice prints;
17. Full face photographic images and any comparable images; and
18. Any other unique identifying number, characteristic, or code (note this does not mean the unique code assigned by the investigator to code the data)
There are also additional standards and criteria to protect individuals from re-identification. Any code used to replace the identifiers in data sets cannot be derived from any information related to the individual and the master codes, nor can the method to derive the codes be disclosed. For example, a subjects initials cannot be used to code their data because the initials are derived from their name. Additionally, the researcher must not have actual knowledge that the research subject could be re-identified from the remaining identifiers in the PHI used in the research study. In other words, the information would still be considered identifiable if there was a way to identify the individual even though all of the 18 identifiers were removed.

Additional entities to count as PII:
1. Hospital and Facility Names -- any named healthcare facility, including hospitals,
   clinics, centers, and their abbreviations (e.g., "Fairm of Ijordcompmac Hospital",
   "FIH", "Massachusetts General Hospital", "Cancer Center", "Oncology Clinic", "ELMVH")

Pay special attention to these commonly missed entity types:
- Medical Record Numbers (MRNs): numeric codes of 5-10 digits, often prefixed with "MRN:", or appearing near the top of clinical notes (e.g., "0408267", "957770228", "46769/5v7d")
- Certificate/License Numbers: codes following "MD", "DO", "NP", "RN" or labeled as license/certificate numbers
- Hospital/Facility Names: full names AND abbreviations, including those with suffixes
  like "Center", "Clinic", "Unit", "Institute" (e.g., "FIH", "Cancer Center", "ELMVH")

When the text is financial or business-related, also pay special attention to:
- Organization/Company Names: banks, insurers, law firms, investment firms, and any named business entity (e.g., "Heritage Trust Bank", "Trident Manufacturing Inc.", "Pinnacle Wealth Advisors")
- Financial Account Numbers: sequences of digits possibly separated by dashes (e.g., "8820-5567-1243")
- Reference/Case IDs: prefixed alphanumeric codes used as identifiers (e.g., "AP-2024-09-3382", "WIRE-2024-081590", "CLM-2024-08-29471", "EMP-04417")
- EIN/TIN: employer or tax identification numbers in NN-NNNNNNN format (e.g., "95-4281037")

You will identify all PHI with the following enums as the "label":

{label_enums}

Respond with a list of dictionaries such as [{{"entity": "Alice Anderson", "entity_type": "PERSON"}}, {{"entity": "123-45-6789", "entity_type": "US_SSN"}}]

IMPORTANT: Return each entity EXACTLY as it appears in the original text. Copy the text character-for-character -- do not normalize whitespace, fix spelling, or rephrase. If the text contains "John\nSmith", return "John\nSmith", not "John Smith".

List every occurrence of each entity separately. For example, if the text says "The patient, Brennan, notes that is feeling unwell. Brennan presents with a moderate fever of 100.5F," list the entity "Brennan" twice. Do not skip repeated mentions.

The text is listed here: 
<MedicalText>
{{med_text}}
<MedicalText/>

EXAMPLES: 
MedicalText: "MRN: 222345 -- I saw patient Alice Anderson today at 11:30am at Springfield General Hospital, who presents with a sore throat and temperature of 103F"
response: [{{"entity": "Alice Anderson", "entity_type": "PERSON"}}, {{"entity": "222345", "entity_type": "MEDICAL_RECORD_NUMBER"}}, {{"entity": "Springfield General Hospital", "entity_type": "HOSPITAL_NAME"}}]

MedicalText: "957770228\nFIH\n0408267\n46769/5v7d\nADMISSION DATE: 2-5-94"
response: [{{"entity": "957770228", "entity_type": "MEDICAL_RECORD_NUMBER"}}, {{"entity": "FIH", "entity_type": "HOSPITAL_NAME"}}, {{"entity": "0408267", "entity_type": "MEDICAL_RECORD_NUMBER"}}, {{"entity": "46769/5v7d", "entity_type": "MEDICAL_RECORD_NUMBER"}}, {{"entity": "2-5-94", "entity_type": "DATE_TIME"}}]

MedicalText: "Patient was referred from Elm Valley Cancer Center by Dr. Page Forrestine for further evaluation at ELMVH."
response: [{{"entity": "Elm Valley Cancer Center", "entity_type": "HOSPITAL_NAME"}}, {{"entity": "Page Forrestine", "entity_type": "PERSON"}}, {{"entity": "ELMVH", "entity_type": "HOSPITAL_NAME"}}]

MedicalText: "RE: WIRE-2024-081590 -- Wire transfer of $50,000 from account 8820-5567-1243 at Heritage Trust Bank. Contact: Lisa Chen, EIN 95-4281037."
response: [{{"entity": "WIRE-2024-081590", "entity_type": "ID_NUMBER"}}, {{"entity": "8820-5567-1243", "entity_type": "ID_NUMBER"}}, {{"entity": "Heritage Trust Bank", "entity_type": "ORGANIZATION"}}, {{"entity": "Lisa Chen", "entity_type": "PERSON"}}, {{"entity": "95-4281037", "entity_type": "ID_NUMBER"}}]

MedicalText: "SSN 123-45-6789. DOB: 24/07/1974. Phone: 608-555-7714. MRN: 0408267. Follow-up scheduled 15/03/25."
response: [{{"entity": "123-45-6789", "entity_type": "US_SSN"}}, {{"entity": "24/07/1974", "entity_type": "DATE_TIME"}}, {{"entity": "608-555-7714", "entity_type": "PHONE_NUMBER"}}, {{"entity": "0408267", "entity_type": "MEDICAL_RECORD_NUMBER"}}, {{"entity": "15/03/25", "entity_type": "DATE_TIME"}}]
"""

# Default thresholds
DEFAULT_PRESIDIO_SCORE_THRESHOLD = 0.7
DEFAULT_FUZZY_MATCH_THRESHOLD = 50
DEFAULT_OVERLAP_TOLERANCE = 0

# AI Query defaults
# AI entities get a default confidence of 0.8 since the LLM does not provide a per-entity score
DEFAULT_AI_CONFIDENCE_SCORE = 0.8
DEFAULT_AI_REASONING_EFFORT = "low"  # Valid: "low", "medium", "high"

# GLiNER defaults
# Labels must match the nvidia/nemotron-pii training data (lowercase with underscores).
# The model uses these as zero-shot prompts, so exact label text matters.
DEFAULT_GLINER_MODEL = "nvidia/gliner-PII"
DEFAULT_GLINER_LABELS = [
    "name",
    "first_name",
    "last_name",
    "email",
    "phone_number",
    "fax_number",
    "street_address",
    "city",
    "state",
    "county",
    "country",
    "postcode",
    "ssn",
    "national_id",
    "tax_id",
    "date",
    "date_of_birth",
    "date_time",
    "age",
    "medical_record_number",
    "health_plan_beneficiary_number",
    "credit_debit_card",
    "account_number",
    "bank_routing_number",
    "certificate_license_number",
    "vehicle_identifier",
    "license_plate",
    "url",
    "ip_address",
    "mac_address",
    "company_name",
    "hospital_or_medical_facility",
    "bank",
    "customer_id",
    "unique_identifier",
    "employee_id",
    "pin",
]

# Maps nemotron-pii training labels -> standardized output entity types.
# Multiple input labels can map to the same output type.
GLINER_LABEL_MAP = {
    "name": "PERSON",
    "first_name": "PERSON",
    "last_name": "PERSON",
    "email": "EMAIL_ADDRESS",
    "phone_number": "PHONE_NUMBER",
    "fax_number": "PHONE_NUMBER",
    "street_address": "LOCATION",
    "city": "LOCATION",
    "state": "LOCATION",
    "county": "LOCATION",
    "country": "LOCATION",
    "postcode": "LOCATION",
    "ssn": "US_SSN",
    "national_id": "ID_NUMBER",
    "tax_id": "ID_NUMBER",
    "date": "DATE_TIME",
    "date_of_birth": "DATE_TIME",
    "date_time": "DATE_TIME",
    "age": "AGE",
    "medical_record_number": "MEDICAL_RECORD_NUMBER",
    "health_plan_beneficiary_number": "HEALTH_PLAN_NUMBER",
    "credit_debit_card": "CREDIT_CARD",
    "account_number": "ACCOUNT_NUMBER",
    "bank_routing_number": "BANK_ROUTING_NUMBER",
    "certificate_license_number": "LICENSE_NUMBER",
    "vehicle_identifier": "VEHICLE_IDENTIFIER",
    "license_plate": "VEHICLE_IDENTIFIER",
    "url": "URL",
    "ip_address": "IP_ADDRESS",
    "mac_address": "MAC_ADDRESS",
    "company_name": "ORGANIZATION",
    "hospital_or_medical_facility": "HOSPITAL_NAME",
    "bank": "ORGANIZATION",
    "customer_id": "CUSTOMER_ID",
    "unique_identifier": "ID_NUMBER",
    "employee_id": "ID_NUMBER",
    "pin": "PIN",
}
DEFAULT_GLINER_THRESHOLD = 0.2
DEFAULT_GLINER_MAX_WORDS = 256

DEFAULT_GLINER_THRESHOLDS_BY_TYPE = {
    # Keys must match DEFAULT_GLINER_LABELS exactly
    "name": 0.15,
    "first_name": 0.15,
    "last_name": 0.15,
    "phone_number": 0.3,
    "fax_number": 0.3,
    "email": 0.3,
    "ssn": 0.4,
    "national_id": 0.35,
    "tax_id": 0.35,
    "date": 0.2,
    "date_of_birth": 0.25,
    "date_time": 0.2,
    "age": 0.3,
    "street_address": 0.2,
    "city": 0.2,
    "state": 0.25,
    "county": 0.25,
    "country": 0.25,
    "postcode": 0.3,
    "medical_record_number": 0.35,
    "health_plan_beneficiary_number": 0.35,
    "credit_debit_card": 0.4,
    "account_number": 0.35,
    "bank_routing_number": 0.35,
    "certificate_license_number": 0.35,
    "vehicle_identifier": 0.35,
    "license_plate": 0.35,
    "url": 0.3,
    "ip_address": 0.3,
    "mac_address": 0.3,
    "company_name": 0.3,
    "hospital_or_medical_facility": 0.25,
    "bank": 0.3,
    "customer_id": 0.35,
    "unique_identifier": 0.35,
    "employee_id": 0.35,
    "pin": 0.4,
}

# Confidence thresholds
HIGH_CONFIDENCE_THRESHOLD = 0.7

# Source weights for confidence calculation (used in weighted scoring)
SOURCE_WEIGHTS = {
    "presidio": 0.35,
    "gliner": 0.30,
    "ai": 0.35,
}

# Match quality thresholds
EXACT_MATCH_SCORE = 1.0
OVERLAP_MATCH_SCORE = 0.7
FUZZY_MATCH_SCORE = 0.5

# Confidence levels based on weighted scores
CONFIDENCE_THRESHOLDS = {
    "high": 0.7,  # 2+ sources with high agreement
    "medium": 0.4,  # 1-2 sources with partial agreement
    "low": 0.0,  # Single source or low agreement
}

# Required entity fields (minimal set needed for alignment)
REQUIRED_ENTITY_FIELDS = {"entity", "start", "end"}

# ---------------------------------------------------------------------------
# Post-detection entity filtering
# ---------------------------------------------------------------------------
# Entity types to suppress entirely (matched against the detection entity_type).
ENTITY_TYPES_TO_IGNORE = {
    "NRP",  # Nationalities / religious / political groups -- not PII in most contexts
}

# Regex patterns matched against the detected entity *text* (case-insensitive).
# Any entity whose text fully matches one of these is dropped.
import re as _re  # noqa: E402
ENTITY_TEXT_IGNORE_PATTERNS = _re.compile(
    r"^("
    r"dr\.?|mr\.?|mrs\.?|ms\.?|prof\.?|sir|madam"  # titles / prefixes
    r"|he|she|they|you|we|it|i"                       # pronouns
    r"|nurse|physician|surgeon"                        # generic clinical roles (not "doctor"/"patient" -- can appear in names)
    r"|am|pm"                                          # bare time fragments
    r"|today|yesterday|tomorrow"                         # relative day words
    r"|(19|20)\d{2}"                                     # bare 4-digit years
    r"|.*\bago"                                          # "approximately one hour ago" etc.
    r"|(?:post)?operative\s+day\s+\w+"                   # "postoperative day two"
    # Relative time durations -- not identifying dates under Safe Harbor
    r"|daily|weekly|monthly|annually|hourly|nightly|quarterly|biweekly|bimonthly"
    r"|\d+\s*(?:min(?:ute)?s?|hrs?|hours?|days?|weeks?|months?|years?|nights?)(?:\s*old)?"
    r"|(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)"
    r"\s+(?:days?|weeks?|months?|years?|hours?|minutes?|nights?)(?:\s*old)?"
    r"|past\s+\d+\s+(?:months?|years?|days?|weeks?)"
    r")$",
    _re.IGNORECASE,
)

# Short all-caps strings (2-3 chars) tagged as LOCATION are almost always
# country/state codes (US, UK, CA, NYC) rather than identifying locations.
_SHORT_LOCATION_RE = _re.compile(r"^[A-Z]{2,3}$")


_LOOKS_LIKE_ID_RE = _re.compile(r"(?:\d|[A-Z].*[a-z]|[a-z].*[A-Z]|.*\.)")


def should_ignore_entity(
    entity_text: str,
    entity_type: str,
    types_to_ignore: set = None,
) -> bool:
    """Return True if the entity should be dropped from results.

    Args:
        types_to_ignore: Override the default ``ENTITY_TYPES_TO_IGNORE`` set.
    """
    if types_to_ignore is None:
        types_to_ignore = ENTITY_TYPES_TO_IGNORE
    stripped = entity_text.strip()
    if entity_type in types_to_ignore:
        return True
    if len(stripped) <= 1:
        return True
    if len(stripped) == 2 and not _LOOKS_LIKE_ID_RE.match(stripped):
        return True
    if ENTITY_TEXT_IGNORE_PATTERNS.match(stripped):
        return True
    if entity_type == "LOCATION" and _SHORT_LOCATION_RE.match(stripped):
        return True
    return False


# ---------------------------------------------------------------------------
# Prompt version tracking (short hash of PHI_PROMPT_SKELETON for audit)
# ---------------------------------------------------------------------------
PROMPT_VERSION = _hashlib.sha256(PHI_PROMPT_SKELETON.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Pipeline configuration dataclass
# ---------------------------------------------------------------------------
@dataclass
class RedactionConfig:
    """Single configuration object for the redaction pipeline.

    Pass to ``run_redaction_pipeline(config=...)`` instead of listing 25+
    keyword arguments.  Individual kwargs on the pipeline functions still work
    and take precedence when both are supplied.
    """
    # Detection
    use_presidio: bool = True
    use_ai_query: bool = True
    use_gliner: bool = False
    endpoint: Optional[str] = None
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD
    gliner_model: str = DEFAULT_GLINER_MODEL
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD
    gliner_max_words: Optional[int] = None
    num_cores: int = 10
    fail_on_presidio_error: bool = True
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT
    presidio_model_size: Optional[str] = None
    presidio_pattern_only: bool = False
    ai_model_type: str = "foundation"
    # Alignment
    alignment_mode: str = "union"
    fuzzy_threshold: int = 50
    allow_consensus_redaction: bool = False
    # Redaction
    redaction_strategy: str = "generic"
    output_strategy: str = "production"
    output_mode: str = "separate"
    confirm_destructive: bool = False
    confirm_validation_output: bool = False
    max_rows: Optional[int] = 10000
    entity_filter: Any = field(default=None, repr=False)

    def __post_init__(self):
        if self.score_threshold < MIN_SCORE_THRESHOLD:
            raise ValueError(
                f"score_threshold={self.score_threshold} is below the governance "
                f"floor of {MIN_SCORE_THRESHOLD}. Very low thresholds can silently "
                f"disable detection."
            )
        if self.gliner_threshold < MIN_GLINER_THRESHOLD:
            raise ValueError(
                f"gliner_threshold={self.gliner_threshold} is below the governance "
                f"floor of {MIN_GLINER_THRESHOLD}."
            )
        if self.score_threshold == MIN_SCORE_THRESHOLD:
            _logger.warning(
                "score_threshold is at governance floor (%s). This should be exceptional.",
                MIN_SCORE_THRESHOLD,
            )
        if self.gliner_threshold == MIN_GLINER_THRESHOLD:
            _logger.warning(
                "gliner_threshold is at governance floor (%s). This should be exceptional.",
                MIN_GLINER_THRESHOLD,
            )


# ---------------------------------------------------------------------------
# Judge prompt -- grades redaction quality by comparing original vs redacted
# ---------------------------------------------------------------------------
JUDGE_PROMPT_SKELETON = """
You are an expert auditor of Protected Health Information (PHI) redaction.

You will receive an ORIGINAL medical text and a REDACTED version of that text.
Your job is to identify any PHI that was MISSED (still present in the redacted
text exactly as in the original) or PARTIALLY_MISSED (only part of the entity
was redacted, e.g. first name redacted but last name remains).

PHI categories to check: Names, Dates, Phone/Fax numbers, Emails, SSNs,
Medical record numbers, Health plan numbers, Account numbers, License/certificate
numbers, Vehicle identifiers, Device identifiers, URLs, IP addresses, Biometric
identifiers, Locations smaller than State, Ages over 89, Hospital names.

Respond with a JSON object with two keys:
1. "grade": one of "PASS" (no remaining PHI), "PARTIAL" (some PHI missed),
   or "FAIL" (significant PHI remaining).
2. "findings": a list of objects, each with:
   - "entity": the PHI text still visible in the redacted version
   - "entity_type": category from the list above
   - "status": "MISSED" or "PARTIALLY_MISSED"
   - "explanation": brief reason

Important grading context:
- Standalone day names (Monday, Tuesday) are NOT PHI unless part of a full date.
- Generic titles (Dr., M.D.) and pronouns (he, she) are NOT PHI.
- Short abbreviations for hospitals (e.g., FIH, MGH) are borderline -- do not
  penalize for missing these unless the full hospital name is also missed.
- Gender terms (male, female) alone are NOT one of the 18 HIPAA identifiers.
- Standalone 4-digit years (e.g. 1978, 2024) are NOT PHI under HIPAA Safe Harbor,
  which explicitly permits year to remain. Only flag years when they appear as part
  of a full date (e.g. "March 15, 1978").
- Grade as PASS if only borderline/debatable items remain.

If no PHI remains, return {{"grade": "PASS", "findings": []}}.

<OriginalText>
{{original_text}}
</OriginalText>

<RedactedText>
{{redacted_text}}
</RedactedText>
"""

# ---------------------------------------------------------------------------
# Next-action recommender prompt
# ---------------------------------------------------------------------------
NEXT_ACTION_PROMPT_SKELETON = """
You are a PHI/PII detection engineering advisor. Given the benchmark results
below, recommend the top 3-5 specific, actionable improvements.

CONSTRAINTS -- follow these strictly:
- Every recommendation must be SAFE: it must not risk a large increase in false
  positives. If you suggest lowering a threshold, quantify the expected FP cost.
- Focus on changes to configuration values, entity filter patterns, and prompt
  wording. Do NOT recommend replacing models, switching libraries, or major
  architectural rewrites.
- The system has an entity text ignore filter (regex patterns and entity-type
  blocklist in config.py). Adding patterns there is a valid, low-risk action.
- The "aligned" method IS the ensemble -- it combines presidio, ai, and gliner.
  Do not recommend creating a separate ensemble.
- Aligned recall is the single most important metric because aligned output is
  what gets used for redaction. Improving individual method recall feeds aligned.
- Some ground truth entities (3-letter hospital abbreviations, bare number
  sequences, standalone day names) represent a benchmark ceiling that no general
  model will reach. Factor this into recall expectations.

Respond with a JSON list of objects, each with:
- "priority": 1-5 (1 = highest)
- "method": which detection method this applies to (presidio, ai, gliner, aligned, or all)
- "action": concise description of the change (one sentence)
- "rationale": why this will help, referencing specific metrics or FP/FN patterns
  and the expected precision/recall tradeoff

<BenchmarkContext>
{{context}}
</BenchmarkContext>
"""
