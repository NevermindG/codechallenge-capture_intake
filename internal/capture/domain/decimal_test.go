package domain

import (
	"encoding/json"
	"testing"
)

func TestDecimalRoundTripPreservesExactValue(t *testing.T) {
	original, err := ParseDecimal("1234567890.120300")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `"1234567890.120300"` {
		t.Fatalf("unexpected JSON decimal: %s", encoded)
	}
	var decoded Decimal
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !original.Equal(decoded) || original.String() != decoded.String() {
		t.Fatalf("decimal did not round-trip exactly: %s -> %s", original.String(), decoded.String())
	}
}

func TestDecimalRejectsJSONNumber(t *testing.T) {
	var d Decimal
	if err := json.Unmarshal([]byte(`123.45`), &d); err == nil {
		t.Fatal("expected JSON number to be rejected so no float enters the path")
	}
}

func TestParseDecimalRejectsDatabasePrecisionOverflow(t *testing.T) {
	tooLarge := "12345678901234567890123456789.00" // 29 integer digits; NUMERIC(38,12) allows 26.
	if _, err := ParseDecimal(tooLarge); err == nil {
		t.Fatal("expected precision error")
	}
}

func TestParseDecimalAcceptsMaximumDatabasePrecision(t *testing.T) {
	max := "99999999999999999999999999.999999999999" // 26 integer + 12 fractional digits.
	if _, err := ParseDecimal(max); err != nil {
		t.Fatalf("maximum supported decimal rejected: %v", err)
	}
}
