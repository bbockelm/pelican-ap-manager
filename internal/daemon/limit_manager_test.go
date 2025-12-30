package daemon

import "testing"

func TestParseLimitExpression(t *testing.T) {
	tests := []struct {
		name          string
		expr          string
		siteAttribute string
		wantUser      string
		wantSite      string
		wantOk        bool
	}{
		{
			name:          "old format",
			expr:          `(User == "alice" && TARGET.GLIDEIN_Site == "UW-Madison")`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "alice",
			wantSite:      "UW-Madison",
			wantOk:        true,
		},
		{
			name:          "new format without sources",
			expr:          `(JOB.Owner =?= "bob" && TARGET.GLIDEIN_Site =?= "UCSD")`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "bob",
			wantSite:      "UCSD",
			wantOk:        true,
		},
		{
			name:          "new format with sources",
			expr:          `(JOB.Owner =?= "charlie" && stringListMember("osdf:///ospool/ap40", JOB.PelicanInputPrefixes ?: "") && TARGET.GLIDEIN_Site =?= "Rhodes-HPC")`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "charlie",
			wantSite:      "Rhodes-HPC",
			wantOk:        true,
		},
		{
			name:          "multiple sources",
			expr:          `(JOB.Owner =?= "dave" && (stringListMember("osdf:///ospool/ap40", JOB.PelicanInputPrefixes ?: "") || stringListMember("stash:///osgconnect", JOB.PelicanInputPrefixes ?: "")) && TARGET.GLIDEIN_Site =?= "UConn")`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "dave",
			wantSite:      "UConn",
			wantOk:        true,
		},
		{
			name:          "invalid expression",
			expr:          `not a valid expression`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "",
			wantSite:      "",
			wantOk:        false,
		},
		{
			name:          "missing user",
			expr:          `(TARGET.GLIDEIN_Site == "MIT")`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "",
			wantSite:      "MIT",
			wantOk:        false,
		},
		{
			name:          "missing site",
			expr:          `(JOB.Owner =?= "eve")`,
			siteAttribute: "GLIDEIN_Site",
			wantUser:      "eve",
			wantSite:      "",
			wantOk:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotSite, gotOk := parseLimitExpression(tt.expr, tt.siteAttribute)
			if gotUser != tt.wantUser {
				t.Errorf("parseLimitExpression() user = %v, want %v", gotUser, tt.wantUser)
			}
			if gotSite != tt.wantSite {
				t.Errorf("parseLimitExpression() site = %v, want %v", gotSite, tt.wantSite)
			}
			if gotOk != tt.wantOk {
				t.Errorf("parseLimitExpression() ok = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
