package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONMasksConfiguredFields(t *testing.T) {
	r := New([]string{"account_number", "  ", "SSN"})
	in := json.RawMessage(`{"amount":100,"accountNumber":"12345678","nested":{"Account-Number":"9","ssn":"1"},"list":[{"ssn":"2","ok":true}]}`)

	got := map[string]any{}
	if err := json.Unmarshal(r.JSON(in), &got); err != nil {
		t.Fatalf("decode redacted: %v", err)
	}
	if got["accountNumber"] != Mask {
		t.Fatalf("accountNumber = %v, want %s", got["accountNumber"], Mask)
	}
	if got["amount"] != float64(100) {
		t.Fatalf("amount = %v, want 100", got["amount"])
	}
	nested := got["nested"].(map[string]any)
	if nested["Account-Number"] != Mask || nested["ssn"] != Mask {
		t.Fatalf("nested = %v, want both masked", nested)
	}
	list := got["list"].([]any)[0].(map[string]any)
	if list["ssn"] != Mask || list["ok"] != true {
		t.Fatalf("list[0] = %v, want ssn masked and ok kept", list)
	}
}

func TestNilRedactorPassesThrough(t *testing.T) {
	var r *Redactor
	in := json.RawMessage(`{"ssn":"1"}`)
	if got := r.JSON(in); string(got) != string(in) {
		t.Fatalf("JSON() = %s, want %s", got, in)
	}
	if New(nil) != nil {
		t.Fatal("New(nil) should return a nil Redactor")
	}
	if New([]string{"  ", ""}) != nil {
		t.Fatal("New() with only blank fields should return a nil Redactor")
	}
}

func TestUnparsableInputIsMaskedWhole(t *testing.T) {
	r := New([]string{"ssn"})
	got := string(r.JSON(json.RawMessage(`{not json`)))
	if !strings.Contains(got, Mask) {
		t.Fatalf("JSON() = %s, want it masked whole", got)
	}
}

func TestSplit(t *testing.T) {
	got := Split(" account_number , ssn ,, ")
	if len(got) != 2 || got[0] != "account_number" || got[1] != "ssn" {
		t.Fatalf("Split() = %#v", got)
	}
}
