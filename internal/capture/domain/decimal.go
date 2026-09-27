package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"regexp"
	"strings"
)

const (
	MaxDecimalScale         = 12
	MaxDecimalIntegerDigits = 26 // NUMERIC(38,12) => 38 total precision - 12 fractional digits.
)

var decimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

type Decimal struct {
	unscaled big.Int
	scale    int
}

func ParseDecimal(s string) (Decimal, error) {
	var d Decimal
	if s == "" || !decimalPattern.MatchString(s) {
		return d, errors.New("decimal must be a non-negative plain decimal string")
	}
	parts := strings.SplitN(s, ".", 2)
	integerPart := parts[0]
	fractionPart := ""
	if len(parts) == 2 {
		fractionPart = parts[1]
	}
	if len(fractionPart) > MaxDecimalScale {
		return d, errors.New("decimal has too many fractional digits")
	}
	integerDigits := len(integerPart)
	if integerPart == "0" {
		integerDigits = 1
	}
	if integerDigits > MaxDecimalIntegerDigits {
		return d, errors.New("decimal exceeds the supported numeric precision")
	}
	combined := integerPart + fractionPart
	if _, ok := d.unscaled.SetString(combined, 10); !ok {
		return Decimal{}, errors.New("invalid decimal digits")
	}
	d.scale = len(fractionPart)
	return d, nil
}

func (d Decimal) IsZero() bool {
	return d.unscaled.Sign() == 0
}

func (d Decimal) String() string {
	digits := d.unscaled.String()
	if d.scale == 0 {
		return digits
	}
	negative := strings.HasPrefix(digits, "-")
	if negative {
		digits = digits[1:]
	}
	if len(digits) <= d.scale {
		digits = strings.Repeat("0", d.scale-len(digits)+1) + digits
	}
	cut := len(digits) - d.scale
	out := digits[:cut] + "." + digits[cut:]
	if negative {
		out = "-" + out
	}
	return out
}

func (d Decimal) Equal(other Decimal) bool {
	left := new(big.Int).Set(&d.unscaled)
	right := new(big.Int).Set(&other.unscaled)
	if d.scale > other.scale {
		right.Mul(right, pow10(d.scale-other.scale))
	} else if other.scale > d.scale {
		left.Mul(left, pow10(other.scale-d.scale))
	}
	return left.Cmp(right) == 0
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func (d Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Decimal) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("decimal must be a JSON string")
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := ParseDecimal(s)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}
