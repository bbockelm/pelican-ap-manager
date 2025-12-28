package epochhistory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// Generate builds an epoch_history-style ClassAd log from sanitized JSON inputs.
// jobEpochsPath should contain job completion ads; transferAdsPath should contain
// input/output transfer ads with plugin result lists. Timestamps are rebased so
// the newest EpochWriteDate aligns to the provided clock.
func Generate(outputPath, jobEpochsPath, transferAdsPath string, now time.Time) error {
	jobAds, err := loadJSONAds(jobEpochsPath)
	if err != nil {
		return fmt.Errorf("load job epochs: %w", err)
	}

	transferAds, err := loadJSONAds(transferAdsPath)
	if err != nil {
		return fmt.Errorf("load transfer ads: %w", err)
	}

	allAds := make([]*classad.ClassAd, 0, len(jobAds)+len(transferAds))
	allAds = append(allAds, jobAds...)
	allAds = append(allAds, transferAds...)
	allAds = append(allAds, synthesizeSpawnAds(jobAds)...)

	shift := computeShift(allAds, now)
	for _, ad := range allAds {
		if err := rebaseTimestamps(ad, shift); err != nil {
			return fmt.Errorf("rebase ad: %w", err)
		}
		if err := normalizeAdType(ad); err != nil {
			return fmt.Errorf("normalize ad: %w", err)
		}
	}

	sortAds(allAds)
	if err := writeOldLog(outputPath, allAds); err != nil {
		return fmt.Errorf("write epoch_history: %w", err)
	}

	return nil
}

func loadJSONAds(path string) ([]*classad.ClassAd, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var blobs []json.RawMessage
	if err := json.Unmarshal(data, &blobs); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", path, err)
	}

	ads := make([]*classad.ClassAd, 0, len(blobs))
	for i, b := range blobs {
		ad := classad.New()
		if err := json.Unmarshal(b, ad); err != nil {
			return nil, fmt.Errorf("unmarshal %s[%d]: %w", path, i, err)
		}
		ads = append(ads, ad)
	}
	return ads, nil
}

func synthesizeSpawnAds(jobAds []*classad.ClassAd) []*classad.ClassAd {
	out := make([]*classad.ClassAd, 0, len(jobAds))
	for _, job := range jobAds {
		cluster := attrInt64(job, "ClusterId", "ClusterID")
		proc := attrInt64(job, "ProcId", "ProcID")
		if cluster == 0 {
			continue
		}

		owner := attrString(job, "Owner", "User")
		start := attrInt64(job, "JobCurrentStartDate", "JobStartDate")
		shadow := attrInt64(job, "ShadowBday")
		if shadow == 0 {
			shadow = start
		}
		runID := attrInt64(job, "RunInstanceID")
		starts := attrInt64(job, "NumShadowStarts")
		if starts == 0 {
			starts = 1
		}

		ad := classad.New()
		_ = ad.Set("ClusterId", cluster)
		_ = ad.Set("ProcId", proc)
		_ = ad.Set("RunInstanceID", runID)
		_ = ad.Set("Owner", owner)
		_ = ad.Set("ShadowBday", shadow)
		_ = ad.Set("NumShadowStarts", starts)
		_ = ad.Set("EpochAdType", "SPAWN")
		_ = ad.Set("EpochWriteDate", start)
		out = append(out, ad)
	}
	return out
}

func computeShift(ads []*classad.ClassAd, now time.Time) int64 {
	if now.IsZero() {
		now = time.Now()
	}

	var maxTS int64
	for _, ad := range ads {
		ts := attrInt64(ad, "EpochWriteDate")
		if ts > maxTS {
			maxTS = ts
		}
	}

	if maxTS == 0 {
		return 0
	}

	delta := now.Unix() - maxTS
	if delta < 0 {
		return 0
	}
	return delta
}

func rebaseTimestamps(ad *classad.ClassAd, shift int64) error {
	m, err := adToMap(ad)
	if err != nil {
		return err
	}
	rebaseTimestampsMap(m, shift)
	return mapToAd(m, ad)
}

func rebaseTimestampsMap(node any, shift int64) {
	switch v := node.(type) {
	case map[string]any:
		for k, val := range v {
			if shifted, ok := shiftValue(k, val, shift); ok {
				v[k] = shifted
			} else {
				rebaseTimestampsMap(val, shift)
			}
		}
	case []any:
		for i, item := range v {
			if shifted, ok := shiftValue("", item, shift); ok {
				v[i] = shifted
			} else {
				rebaseTimestampsMap(item, shift)
			}
		}
	}
}

func shiftValue(key string, val any, shift int64) (any, bool) {
	switch num := val.(type) {
	case float64:
		if isEpochishKey(key) && num >= 1e9 {
			return num + float64(shift), true
		}
	case int64:
		if isEpochishKey(key) && num >= 1e9 {
			return num + shift, true
		}
	case json.Number:
		if isEpochishKey(key) {
			if i, err := num.Int64(); err == nil && i >= 1e9 {
				return i + shift, true
			}
		}
	}
	return nil, false
}

func isEpochishKey(key string) bool {
	if key == "" {
		return false
	}
	key = strings.ToLower(key)
	return strings.Contains(key, "date") || strings.Contains(key, "time") || key == "epochwritedate"
}

func normalizeAdType(ad *classad.ClassAd) error {
	if raw := attrString(ad, "EpochAdType"); raw != "" {
		return ad.Set("EpochAdType", strings.ToUpper(raw))
	}

	if tc := attrString(ad, "TransferClass"); tc != "" {
		return ad.Set("EpochAdType", strings.ToUpper(tc))
	}

	if tt := attrString(ad, "TransferType"); tt != "" {
		return ad.Set("EpochAdType", strings.ToUpper(tt))
	}

	return nil
}

func writeOldLog(path string, ads []*classad.ClassAd) error {
	var buf bytes.Buffer
	for _, ad := range ads {
		text := ad.MarshalOld()
		buf.WriteString(text)
		buf.WriteString("\n")
		buf.WriteString(trailerLine(ad))
		buf.WriteString("\n")
	}

	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func adToMap(ad *classad.ClassAd) (map[string]any, error) {
	payload, err := json.Marshal(ad)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func mapToAd(m map[string]any, ad *classad.ClassAd) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, ad)
}

func trailerLine(ad *classad.ClassAd) string {
	typ := strings.ToUpper(firstStringFromAd(ad, "EpochAdType"))
	cluster, _ := ad.EvaluateAttrInt("ClusterId")
	proc, _ := ad.EvaluateAttrInt("ProcId")
	run, _ := ad.EvaluateAttrInt("RunInstanceID")
	owner := firstStringFromAd(ad, "Owner", "User")
	current, _ := ad.EvaluateAttrInt("EpochWriteDate")

	return fmt.Sprintf("*** %s ClusterId=%d ProcId=%d RunInstanceId=%d Owner=\"%s\" CurrentTime=%d", typ, cluster, proc, run, owner, current)
}

func sortAds(ads []*classad.ClassAd) {
	sort.SliceStable(ads, func(i, j int) bool {
		ai, aj := ads[i], ads[j]
		if d := attrInt64(ai, "EpochWriteDate") - attrInt64(aj, "EpochWriteDate"); d != 0 {
			return d < 0
		}
		if d := attrInt64(ai, "ClusterId", "ClusterID") - attrInt64(aj, "ClusterId", "ClusterID"); d != 0 {
			return d < 0
		}
		if d := attrInt64(ai, "ProcId", "ProcID") - attrInt64(aj, "ProcId", "ProcID"); d != 0 {
			return d < 0
		}
		if d := attrInt64(ai, "RunInstanceID") - attrInt64(aj, "RunInstanceID"); d != 0 {
			return d < 0
		}
		return attrString(ai, "EpochAdType", "TransferClass") < attrString(aj, "EpochAdType", "TransferClass")
	})
}

func attrString(ad *classad.ClassAd, keys ...string) string {
	for _, k := range keys {
		if s, ok := ad.EvaluateAttrString(k); ok {
			return s
		}
	}
	return ""
}

func attrInt64(ad *classad.ClassAd, keys ...string) int64 {
	for _, k := range keys {
		if i, ok := ad.EvaluateAttrInt(k); ok {
			return i
		}
	}
	return 0
}

func firstStringFromAd(ad *classad.ClassAd, keys ...string) string {
	return attrString(ad, keys...)
}
