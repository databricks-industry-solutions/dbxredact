"""Unit tests for detection module."""

import re
import pytest

from dbxredact.config import PHI_PROMPT_SKELETON
from dbxredact.ai_detector import make_prompt
from dbxredact.detection import check_presidio_available
from dbxredact.analyzer import (
    DeaNumberRecognizer, NpiRecognizer, DobLabeledRecognizer,
    FaxNumberRecognizer, HealthPlanIdRecognizer, AccountNumberRecognizer,
    VinRecognizer, MacAddressRecognizer, EinRecognizer,
    SsnNoDashRecognizer, AgeOver89Recognizer, LicenseNumberRecognizer,
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


class TestHIPAASafeHarborPatterns:
    """Verify each HIPAA Safe Harbor regex recognizer matches true positives
    and rejects true negatives. Tests run pure regex -- no spaCy needed."""

    # 1. DEA Number
    @pytest.mark.parametrize("text", ["AB1234567", "fg9876543", "MR0000001"])
    def test_dea_positive(self, text):
        assert _regex(DeaNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", ["XY1234567", "A12345678", "AB123456"])
    def test_dea_negative(self, text):
        assert not _regex(DeaNumberRecognizer).search(text)

    # 2. NPI (labeled)
    @pytest.mark.parametrize("text", ["NPI: 1234567890", "NPI#1234567890", "NPI 9876543210"])
    def test_npi_positive(self, text):
        assert _regex(NpiRecognizer).search(text)

    @pytest.mark.parametrize("text", ["NPI: 12345", "1234567890", "NPI: 12345678901"])
    def test_npi_negative(self, text):
        assert not _regex(NpiRecognizer).search(text)

    # 3. DOB (labeled)
    @pytest.mark.parametrize("text", [
        "DOB: 01/15/1980", "DOB:3-4-92", "D.O.B. 12/31/2001",
    ])
    def test_dob_positive(self, text):
        assert _regex(DobLabeledRecognizer).search(text)

    @pytest.mark.parametrize("text", ["01/15/1980", "Birthday: 01/15/1980"])
    def test_dob_negative(self, text):
        assert not _regex(DobLabeledRecognizer).search(text)

    # 4. Fax Number (labeled)
    @pytest.mark.parametrize("text", [
        "FAX: (555) 123-4567", "Fax: 555.123.4567", "FAX#5551234567",
    ])
    def test_fax_positive(self, text):
        assert _regex(FaxNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", ["Phone: 555-123-4567", "555-123-4567"])
    def test_fax_negative(self, text):
        assert not _regex(FaxNumberRecognizer).search(text)

    # 5. Health Plan / Member ID (labeled)
    @pytest.mark.parametrize("text", [
        "MEMBER ID: ABC1234567", "POLICY# XYZ99887766", "SUBSCRIBER NO 123456",
    ])
    def test_health_plan_positive(self, text):
        assert _regex(HealthPlanIdRecognizer).search(text)

    @pytest.mark.parametrize("text", ["The member spoke about it", "ID: 12"])
    def test_health_plan_negative(self, text):
        assert not _regex(HealthPlanIdRecognizer).search(text)

    # 6. Account Number (labeled)
    @pytest.mark.parametrize("text", [
        "ACCOUNT# 12345678", "ACCT NO: 9876543210", "Account Number: 000123456789",
    ])
    def test_account_positive(self, text):
        assert _regex(AccountNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", ["ACCOUNT# 123", "account of events"])
    def test_account_negative(self, text):
        assert not _regex(AccountNumberRecognizer).search(text)

    # 7. VIN
    @pytest.mark.parametrize("text", ["1HGBH41JXMN109186", "5YJSA1DN5DFP14705"])
    def test_vin_positive(self, text):
        assert _regex(VinRecognizer).search(text)

    @pytest.mark.parametrize("text", ["1HGBH41JXMN10918", "ABCDEFGHIJKLMNOPQ"])
    def test_vin_negative(self, text):
        # too short (16 chars), or contains I/O/Q
        assert not _regex(VinRecognizer).search(text)

    # 8. MAC Address
    @pytest.mark.parametrize("text", [
        "00:1A:2B:3C:4D:5E", "aa-bb-cc-dd-ee-ff", "01:23:45:67:89:AB",
    ])
    def test_mac_positive(self, text):
        assert _regex(MacAddressRecognizer).search(text)

    @pytest.mark.parametrize("text", ["00:1A:2B:3C:4D", "not-a-mac-address", "ZZZZZZZZZZZZZZZZZZ"])
    def test_mac_negative(self, text):
        assert not _regex(MacAddressRecognizer).search(text)

    # 9. US EIN (labeled)
    @pytest.mark.parametrize("text", [
        "EIN: 12-3456789", "TAX ID 98-7654321", "Employer ID: 00-1234567",
    ])
    def test_ein_positive(self, text):
        assert _regex(EinRecognizer).search(text)

    @pytest.mark.parametrize("text", ["12-3456789", "EIN: 123-456789"])
    def test_ein_negative(self, text):
        assert not _regex(EinRecognizer).search(text)

    # 10. SSN without dashes (labeled)
    @pytest.mark.parametrize("text", [
        "SSN: 123456789", "SSN#987654321", "Social Security Number: 111223333",
    ])
    def test_ssn_nodash_positive(self, text):
        assert _regex(SsnNoDashRecognizer).search(text)

    @pytest.mark.parametrize("text", ["123456789", "SSN: 12345678", "SSN: 1234567890"])
    def test_ssn_nodash_negative(self, text):
        assert not _regex(SsnNoDashRecognizer).search(text)

    # 11. Age over 89 (labeled)
    @pytest.mark.parametrize("text", ["Age: 92", "AGE 105", "Aged 90", "age: 150"])
    def test_age_over89_positive(self, text):
        assert _regex(AgeOver89Recognizer).search(text)

    @pytest.mark.parametrize("text", ["Age: 55", "AGE 89", "age: 200"])
    def test_age_over89_negative(self, text):
        assert not _regex(AgeOver89Recognizer).search(text)

    # 12. License / Certificate (labeled)
    @pytest.mark.parametrize("text", [
        "LICENSE# MD12345", "LIC NO: AB123CD", "Certificate Number: X1234Y6789",
    ])
    def test_license_positive(self, text):
        assert _regex(LicenseNumberRecognizer).search(text)

    @pytest.mark.parametrize("text", ["LICENSE# AB", "the license was revoked"])
    def test_license_negative(self, text):
        assert not _regex(LicenseNumberRecognizer).search(text)
