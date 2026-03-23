"""Unit tests for detection module."""

import re
import pytest

from dbxredact.config import PHI_PROMPT_SKELETON
from dbxredact.ai_detector import make_prompt
from dbxredact.detection import check_presidio_available
from dbxredact.analyzer import (
    AgeGenderRecognizer, REFERENCE_NUMBER_REGEX,
    DeaNumberRecognizer, NpiRecognizer, DobLabeledRecognizer,
    FaxNumberRecognizer, HealthPlanIdRecognizer, AccountNumberRecognizer,
    VinRecognizer, MacAddressRecognizer, EinRecognizer,
    SsnNoDashRecognizer, AgeOver89Recognizer, LicenseNumberRecognizer,
    MbiRecognizer, PassportRecognizer, ZipLabeledRecognizer,
    RoutingRecognizer, ItinRecognizer,
)


class TestMakePrompt:
    """Tests for prompt creation."""

    def test_make_prompt_with_list(self):
        prompt = make_prompt(labels=["PERSON", "EMAIL"])
        assert "PERSON" in prompt
        assert "EMAIL" in prompt
        assert "{label_enums}" not in prompt

    def test_make_prompt_with_default(self):
        prompt = make_prompt()
        assert "{label_enums}" not in prompt
        assert len(prompt) > 0

    def test_make_prompt_placeholder_replaced(self):
        template = "Labels: {label_enums}"
        prompt = make_prompt(prompt_skeleton=template, labels=["PERSON"])
        assert prompt == 'Labels: ["PERSON"]'


class TestPresidioAvailability:

    def test_returns_bool_and_optional_message(self):
        is_available, error_msg = check_presidio_available()
        assert isinstance(is_available, bool)
        assert (error_msg is None) == is_available


class TestAIQueryPromptBuilding:
    """Tests for AI Query prompt building (used in streaming)."""

    def test_prompt_contains_med_text_placeholder(self):
        assert "{med_text}" in PHI_PROMPT_SKELETON

    def test_prompt_can_be_split_for_streaming(self):
        prompt = make_prompt()
        parts = prompt.split("{med_text}")
        assert len(parts) == 2
        assert len(parts[0]) > 0

    def test_prompt_escaping_for_sql(self):
        prompt = make_prompt()
        escaped = prompt.replace("'", "''")
        assert "''" in escaped or "'" not in prompt


def _regex(recognizer):
    """Extract the compiled regex from a PatternRecognizer's first pattern."""
    return re.compile(recognizer.patterns[0].regex, re.IGNORECASE)


def _matches_any(recognizer, text):
    """Return True if any pattern in the recognizer matches *text*."""
    return any(re.search(p.regex, text, re.IGNORECASE) for p in recognizer.patterns)


class TestHIPAASafeHarborPatterns:
    """Verify each HIPAA Safe Harbor regex recognizer matches true positives
    and rejects true negatives. Tests run pure regex -- no spaCy needed."""

    # -- 1. DEA Number -------------------------------------------------------
    @pytest.mark.parametrize("text", ["AB1234567", "fg9876543", "MR0000001"])
    def test_dea_positive(self, text):
        assert _regex(DeaNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", ["XY1234567", "A12345678", "AB123456"])
    def test_dea_negative(self, text):
        assert not _regex(DeaNumberRecognizer).search(text)

    # -- 2. NPI (labeled) ----------------------------------------------------
    @pytest.mark.parametrize("text", ["NPI: 1234567890", "NPI#1234567890", "NPI 9876543210"])
    def test_npi_positive(self, text):
        assert _regex(NpiRecognizer).search(text)

    @pytest.mark.parametrize("text", ["NPI: 12345", "1234567890", "NPI: 12345678901"])
    def test_npi_negative(self, text):
        assert not _regex(NpiRecognizer).search(text)

    # -- 3. DOB (labeled) ----------------------------------------------------
    @pytest.mark.parametrize("text", [
        "DOB: 01/15/1980", "DOB:3-4-92", "D.O.B. 12/31/2001",
    ])
    def test_dob_positive(self, text):
        assert _regex(DobLabeledRecognizer).search(text)

    @pytest.mark.parametrize("text", ["01/15/1980", "Birthday: 01/15/1980"])
    def test_dob_negative(self, text):
        assert not _regex(DobLabeledRecognizer).search(text)

    # -- 4. Fax Number (labeled) ---------------------------------------------
    @pytest.mark.parametrize("text", [
        "FAX: (555) 123-4567", "Fax: 555.123.4567", "FAX#5551234567",
    ])
    def test_fax_positive(self, text):
        assert _regex(FaxNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", ["Phone: 555-123-4567", "555-123-4567"])
    def test_fax_negative(self, text):
        assert not _regex(FaxNumberRecognizer).search(text)

    # -- 5. Health Plan / Member ID (labeled) --------------------------------
    @pytest.mark.parametrize("text", [
        "MEMBER ID: ABC1234567", "POLICY# XYZ99887766", "SUBSCRIBER NO 123456",
        "member id: abc1234567",  # case-insensitive
    ])
    def test_health_plan_positive(self, text):
        assert _regex(HealthPlanIdRecognizer).search(text)

    @pytest.mark.parametrize("text", ["The member spoke about it", "ID: 12"])
    def test_health_plan_negative(self, text):
        assert not _regex(HealthPlanIdRecognizer).search(text)

    # -- 6. Account Number (labeled, requires marker) ------------------------
    @pytest.mark.parametrize("text", [
        "ACCOUNT# 12345678", "ACCT NO: 9876543210", "Account Number: 000123456789",
        "account# 12345678",  # case-insensitive
    ])
    def test_account_positive(self, text):
        assert _regex(AccountNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", [
        "ACCOUNT# 123",       # too few digits
        "account of events",  # no label marker
        "ACCOUNT balance 1234567",  # no label marker (balance != NO/NUMBER/#/:)
    ])
    def test_account_negative(self, text):
        assert not _regex(AccountNumberRecognizer).search(text)

    # -- 7. VIN (labeled only) ------------------------------------------------
    @pytest.mark.parametrize("text", [
        "VIN: 1HGBH41JXMN109186", "VIN#5YJSA1DN5DFP14705",
    ])
    def test_vin_positive(self, text):
        assert _regex(VinRecognizer).search(text)

    @pytest.mark.parametrize("text", [
        "1HGBH41JXMN109186",   # unlabeled -- NLP layer's job
        "1HGBH41JXMN10918",    # 16 chars
        "ABCDEFGHIJKLMNOPQ",   # contains I/O/Q
    ])
    def test_vin_negative(self, text):
        assert not _regex(VinRecognizer).search(text)

    # -- 8. MAC Address ------------------------------------------------------
    @pytest.mark.parametrize("text", [
        "00:1A:2B:3C:4D:5E", "aa-bb-cc-dd-ee-ff", "01:23:45:67:89:AB",
    ])
    def test_mac_positive(self, text):
        assert _regex(MacAddressRecognizer).search(text)

    @pytest.mark.parametrize("text", ["00:1A:2B:3C:4D", "not-a-mac-address", "ZZZZZZZZZZZZZZZZZZ"])
    def test_mac_negative(self, text):
        assert not _regex(MacAddressRecognizer).search(text)

    # -- 9. US EIN (labeled) -------------------------------------------------
    @pytest.mark.parametrize("text", [
        "EIN: 12-3456789", "TAX ID 98-7654321", "Employer ID: 00-1234567",
        "ein: 12-3456789",  # case-insensitive
    ])
    def test_ein_positive(self, text):
        assert _regex(EinRecognizer).search(text)

    @pytest.mark.parametrize("text", ["12-3456789", "EIN: 123-456789"])
    def test_ein_negative(self, text):
        assert not _regex(EinRecognizer).search(text)

    # -- 10. SSN without dashes (labeled) ------------------------------------
    @pytest.mark.parametrize("text", [
        "SSN: 123456789", "SSN#987654321", "Social Security Number: 111223333",
        "ssn: 123456789",  # case-insensitive
    ])
    def test_ssn_nodash_positive(self, text):
        assert _regex(SsnNoDashRecognizer).search(text)

    @pytest.mark.parametrize("text", ["123456789", "SSN: 12345678", "SSN: 1234567890"])
    def test_ssn_nodash_negative(self, text):
        assert not _regex(SsnNoDashRecognizer).search(text)

    # -- 11. Age over 89 (labeled) -------------------------------------------
    @pytest.mark.parametrize("text", ["Age: 92", "AGE 105", "Aged 90", "age: 150"])
    def test_age_over89_positive(self, text):
        assert _regex(AgeOver89Recognizer).search(text)

    @pytest.mark.parametrize("text", ["Age: 55", "AGE 89", "age: 200"])
    def test_age_over89_negative(self, text):
        assert not _regex(AgeOver89Recognizer).search(text)

    # -- 12. License / Certificate (labeled, marker mandatory) ---------------
    @pytest.mark.parametrize("text", [
        "LICENSE# MD12345", "LIC NO: AB123CD", "Certificate Number: X1234Y6789",
        "LICENSE 12345ABC",  # digit-start pattern
        "license# MD12345",  # case-insensitive
    ])
    def test_license_positive(self, text):
        assert _matches_any(LicenseNumberRecognizer, text)

    @pytest.mark.parametrize("text", [
        "LICENSE# AB",           # too short
        "the license was revoked",
        "LICENSE AGREEMENT",     # no number-label, value is alpha-only
    ])
    def test_license_negative(self, text):
        assert not _matches_any(LicenseNumberRecognizer, text)

    # -- 13. Medicare Beneficiary Identifier (MBI) ---------------------------
    @pytest.mark.parametrize("text", ["1EG4TE5MK72", "2A93N74WH12"])
    def test_mbi_positive(self, text):
        assert _regex(MbiRecognizer).search(text)

    @pytest.mark.parametrize("text", ["0EG4TE5MK72", "1234567890A", "ABCDEFGHIJK"])
    def test_mbi_negative(self, text):
        assert not _regex(MbiRecognizer).search(text)

    # -- 14. Passport (labeled) ----------------------------------------------
    @pytest.mark.parametrize("text", [
        "PASSPORT: 123456789", "passport# AB12CD34", "Passport C12345",
    ])
    def test_passport_positive(self, text):
        assert _regex(PassportRecognizer).search(text)

    @pytest.mark.parametrize("text", ["123456789", "PASSPORT: AB", "Passport Office"])
    def test_passport_negative(self, text):
        assert not _regex(PassportRecognizer).search(text)

    # -- 15. ZIP code (labeled) ----------------------------------------------
    @pytest.mark.parametrize("text", [
        "ZIP: 90210", "zip code 12345-6789", "ZIP#10001",
    ])
    def test_zip_positive(self, text):
        assert _regex(ZipLabeledRecognizer).search(text)

    @pytest.mark.parametrize("text", ["90210", "ZIP: 1234", "zip 123456"])
    def test_zip_negative(self, text):
        assert not _regex(ZipLabeledRecognizer).search(text)

    # -- 16. Bank routing number (labeled) -----------------------------------
    @pytest.mark.parametrize("text", [
        "ROUTING: 021000021", "ABA# 011401533", "RTN 322271627",
    ])
    def test_routing_positive(self, text):
        assert _regex(RoutingRecognizer).search(text)

    @pytest.mark.parametrize("text", ["021000021", "ROUTING: 4210000210", "routing 12345"])
    def test_routing_negative(self, text):
        assert not _regex(RoutingRecognizer).search(text)

    # -- 17. ITIN (labeled) --------------------------------------------------
    @pytest.mark.parametrize("text", [
        "ITIN: 912-34-5678", "ITIN#900345678", "itin 900123456",
    ])
    def test_itin_positive(self, text):
        assert _regex(ItinRecognizer).search(text)

    @pytest.mark.parametrize("text", ["912-34-5678", "ITIN: 812-34-5678", "ITIN 123"])
    def test_itin_negative(self, text):
        assert not _regex(ItinRecognizer).search(text)


class TestAgeGenderPattern:
    """Age/gender pattern should match realistic ages (10-159) with Y/F/M."""

    @pytest.mark.parametrize("text", ["65F", "32M", "10f", "159m", "99 F", "65Y", "72Y", "100Y"])
    def test_age_gender_positive(self, text):
        assert _regex(AgeGenderRecognizer).search(text)

    @pytest.mark.parametrize("text", [
        "5M",    # single-digit -- likely "5 million"
        "3M",    # company name
        "5Y",    # single-digit
        "9F",    # single-digit
        "160M",  # out of range
    ])
    def test_age_gender_false_positive_rejected(self, text):
        assert not _regex(AgeGenderRecognizer).search(text)


class TestReferenceNumberPattern:
    """Reference number should exclude common medical code prefixes."""

    _REF_PAT = re.compile(REFERENCE_NUMBER_REGEX, re.IGNORECASE)

    @pytest.mark.parametrize("text", [
        "AP-2024-09-3382", "WIRE-2024-081590", "REF-123456",
        "REF-1234", "AP-12345",
    ])
    def test_reference_positive(self, text):
        assert self._REF_PAT.search(text)

    @pytest.mark.parametrize("text", [
        "ICD-10", "CPT-99213", "COVID-19", "DSM-5000", "SARS-12345",
        "HER-20001",
        "ICD-1234567",  # long code -- exercises the lookahead
    ])
    def test_reference_medical_code_rejected(self, text):
        assert not self._REF_PAT.search(text)
