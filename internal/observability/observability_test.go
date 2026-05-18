package observability

import "testing"

func TestNormalizeOTLPEndpoint(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		insecure     bool
		wantEndpoint string
		wantInsecure bool
	}{
		{name: "host port", endpoint: "localhost:4317", insecure: true, wantEndpoint: "localhost:4317", wantInsecure: true},
		{name: "http scheme", endpoint: "http://localhost:4317", insecure: false, wantEndpoint: "localhost:4317", wantInsecure: true},
		{name: "https scheme", endpoint: "https://collector:4317", insecure: true, wantEndpoint: "collector:4317", wantInsecure: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEndpoint, gotInsecure := normalizeOTLPEndpoint(tt.endpoint, tt.insecure)
			if gotEndpoint != tt.wantEndpoint || gotInsecure != tt.wantInsecure {
				t.Fatalf("normalizeOTLPEndpoint() = (%q, %v), want (%q, %v)", gotEndpoint, gotInsecure, tt.wantEndpoint, tt.wantInsecure)
			}
		})
	}
}
