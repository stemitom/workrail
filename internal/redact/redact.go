// Package redact masks configured fields inside stored JSON before it reaches
// a human-facing surface. Workflow payloads routinely carry the values an
// application would never print — account numbers, card PANs, tax IDs — and
// the dashboard renders payloads verbatim to whoever holds the session.
package redact

import (
	"encoding/json"
	"strings"
)

// Mask replaces a redacted value. It is a JSON string so redacted output stays
// valid JSON and still pretty-prints.
const Mask = "[redacted]"

// Redactor masks object members whose key matches one of its fields. Matching
// is case-insensitive and ignores '-' and '_', so "account_number",
// "accountNumber", and "Account-Number" are all one field.
type Redactor struct {
	fields map[string]struct{}
}

// New returns a Redactor for the given field names. With no fields it returns
// nil, and a nil *Redactor passes JSON through untouched.
func New(fields []string) *Redactor {
	set := map[string]struct{}{}
	for _, field := range fields {
		if key := normalize(field); key != "" {
			set[key] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	return &Redactor{fields: set}
}

// Fields returns the configured field names, normalized and unordered. It
// exists so startup can log what is being masked.
func (r *Redactor) Fields() []string {
	if r == nil {
		return nil
	}
	fields := make([]string, 0, len(r.fields))
	for field := range r.fields {
		fields = append(fields, field)
	}
	return fields
}

// JSON returns data with every matching field's value masked. Input that does
// not parse is masked whole rather than passed through: a Redactor is
// configured precisely because this data may not be safe to display.
func (r *Redactor) JSON(data json.RawMessage) json.RawMessage {
	if r == nil || len(data) == 0 {
		return data
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return json.RawMessage(`"` + Mask + `"`)
	}
	masked, err := json.Marshal(r.walk(value))
	if err != nil {
		return json.RawMessage(`"` + Mask + `"`)
	}
	return masked
}

func (r *Redactor) walk(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, member := range typed {
			if _, masked := r.fields[normalize(key)]; masked {
				out[key] = Mask
				continue
			}
			out[key] = r.walk(member)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, member := range typed {
			out[i] = r.walk(member)
		}
		return out
	default:
		return value
	}
}

func normalize(field string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(field)) {
		if r == '-' || r == '_' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Split parses a comma-separated field list, as accepted from configuration
// and the environment.
func Split(value string) []string {
	var fields []string
	for _, field := range strings.Split(value, ",") {
		if field = strings.TrimSpace(field); field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}
