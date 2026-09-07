package main

import "testing"

func TestParseAmount(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "25", want: 2500},
		{in: "25.00", want: 2500},
		{in: " $42.13 ", want: 4213},
		{in: "0.99", want: 99},
		{in: "1.5", want: 150},
		{in: "0", wantErr: true},
		{in: "", wantErr: true},
		{in: "abc", wantErr: true},
	}
	for _, tc := range cases {
		got, err := parseAmount(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseAmount(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseAmount(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("parseAmount(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestLast4(t *testing.T) {
	if got := last4("000123456789"); got != "••••6789" {
		t.Fatalf("last4() = %q", got)
	}
	if got := last4("12"); got != "12" {
		t.Fatalf("last4() short = %q", got)
	}
}

func TestFormatCents(t *testing.T) {
	if got := formatCents(4213); got != "$42.13" {
		t.Fatalf("formatCents() = %q", got)
	}
	if got := formatCents(99); got != "$0.99" {
		t.Fatalf("formatCents() = %q", got)
	}
}

func TestFormatCentsSeparatesThousands(t *testing.T) {
	cases := map[int64]string{
		0: "$0.00", 99: "$0.99", 4213: "$42.13", 100000: "$1,000.00",
		493287: "$4,932.87", 123456789: "$1,234,567.89", -2500: "-$25.00",
	}
	for cents, want := range cases {
		if got := formatCents(cents); got != want {
			t.Fatalf("formatCents(%d) = %q, want %q", cents, got, want)
		}
	}
}
