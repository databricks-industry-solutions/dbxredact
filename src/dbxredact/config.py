"""Configuration constants for PHI/PII detection."""

import hashlib as _hashlib

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
    "NRP",
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

Note that if there are multiple of the same entities, you should list them multiple times. For example, if the text suggests "The patient, Brennan, notes that is feeling unwell. Brennan presents with a moderate fever of 100.5F," you should list the entity "brennan" twice. 

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
"""

# Default thresholds
DEFAULT_PRESIDIO_SCORE_THRESHOLD = 0.7
DEFAULT_FUZZY_MATCH_THRESHOLD = 50
DEFAULT_OVERLAP_TOLERANCE = 0

# AI Query defaults
# AI entities get a default confidence of 0.8 since the LLM does not provide a per-entity score
DEFAULT_AI_CONFIDENCE_SCORE = 0.8
DEFAULT_AI_REASONING_EFFORT = "medium"  # Valid: "low", "medium", "high"

# GLiNER defaults
# Labels must match the nvidia/nemotron-pii training data (lowercase with underscores).
# The model uses these as zero-shot prompts, so exact label text matters.
DEFAULT_GLINER_MODEL = "nvidia/gliner-PII"
DEFAULT_GLINER_LABELS = [
    "first_name",
    "last_name",
    "email",
    "phone_number",
    "street_address",
    "city",
    "state",
    "county",
    "country",
    "ssn",
    "date",
    "date_of_birth",
    "medical_record_number",
    "health_plan_beneficiary_number",
    "credit_debit_card",
    "account_number",
    "bank_routing_number",
    "certificate_license_number",
    "vehicle_identifier",
    "url",
    "ip_address",
    "mac_address",
    "company_name",
    "hospital_or_medical_facility",
    "bank",
    "customer_id",
    "pin",
]

# Maps nemotron-pii training labels -> standardized output entity types.
# Multiple input labels can map to the same output type.
GLINER_LABEL_MAP = {
    "first_name": "PERSON",
    "last_name": "PERSON",
    "email": "EMAIL_ADDRESS",
    "phone_number": "PHONE_NUMBER",
    "street_address": "LOCATION",
    "city": "LOCATION",
    "state": "LOCATION",
    "county": "LOCATION",
    "country": "LOCATION",
    "ssn": "US_SSN",
    "date": "DATE_TIME",
    "date_of_birth": "DATE_TIME",
    "medical_record_number": "MEDICAL_RECORD_NUMBER",
    "health_plan_beneficiary_number": "HEALTH_PLAN_NUMBER",
    "credit_debit_card": "CREDIT_CARD",
    "account_number": "ACCOUNT_NUMBER",
    "bank_routing_number": "BANK_ROUTING_NUMBER",
    "certificate_license_number": "LICENSE_NUMBER",
    "vehicle_identifier": "VEHICLE_IDENTIFIER",
    "url": "URL",
    "ip_address": "IP_ADDRESS",
    "mac_address": "MAC_ADDRESS",
    "company_name": "ORGANIZATION",
    "hospital_or_medical_facility": "LOCATION",
    "bank": "ORGANIZATION",
    "customer_id": "CUSTOMER_ID",
    "pin": "PIN",
}
DEFAULT_GLINER_THRESHOLD = 0.2

DEFAULT_GLINER_THRESHOLDS_BY_TYPE = {
    "person": 0.15,
    "first_name": 0.15,
    "last_name": 0.15,
    "phone number": 0.3,
    "email": 0.3,
    "social security number": 0.4,
    "date of birth": 0.25,
    "address": 0.2,
    "medical record number": 0.35,
    "credit card number": 0.4,
    "health plan beneficiary number": 0.35,
    "account number": 0.35,
    "license number": 0.35,
    "vehicle identifier": 0.35,
    "url": 0.3,
    "ip address": 0.3,
    "biometric identifier": 0.3,
    "full face photograph": 0.3,
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
    r"|doctor|nurse|patient|physician|surgeon"         # generic clinical roles
    r"|am|pm"                                          # bare time fragments
    r"|today|yesterday|tomorrow"                         # relative day words
    r"|(19|20)\d{2}"                                     # bare 4-digit years
    r"|.*\bago"                                          # "approximately one hour ago" etc.
    r"|(?:post)?operative\s+day\s+\w+"                   # "postoperative day two"
    # Relative time durations -- not identifying dates under Safe Harbor
    r"|daily|weekly|monthly|annually|hourly|nightly|quarterly|biweekly|bimonthly"
    r"|\d+\s*(?:min(?:ute)?s?|hrs?|hours?|days?|weeks?|months?|years?|nights?)"
    r"|(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)"
    r"\s+(?:days?|weeks?|months?|years?|hours?|minutes?|nights?)"
    r"|past\s+\d+\s+(?:months?|years?|days?|weeks?)"
    r")$",
    _re.IGNORECASE,
)

# Short all-caps strings (2-3 chars) tagged as LOCATION are almost always
# country/state codes (US, UK, CA, NYC) rather than identifying locations.
_SHORT_LOCATION_RE = _re.compile(r"^[A-Z]{2,3}$")


def should_ignore_entity(entity_text: str, entity_type: str) -> bool:
    """Return True if the entity should be dropped from results."""
    if entity_type in ENTITY_TYPES_TO_IGNORE:
        return True
    if ENTITY_TEXT_IGNORE_PATTERNS.match(entity_text.strip()):
        return True
    if entity_type == "LOCATION" and _SHORT_LOCATION_RE.match(entity_text.strip()):
        return True
    return False


# ---------------------------------------------------------------------------
# Prompt version tracking (short hash of PHI_PROMPT_SKELETON for audit)
# ---------------------------------------------------------------------------
PROMPT_VERSION = _hashlib.sha256(PHI_PROMPT_SKELETON.encode()).hexdigest()[:12]

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
