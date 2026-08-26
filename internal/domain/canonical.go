package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type DurationMillis int64

func DurationToMillis(d time.Duration) DurationMillis {
	return DurationMillis(d / time.Millisecond)
}

func MillisToDuration(v DurationMillis) time.Duration {
	return time.Duration(v) * time.Millisecond
}

func CanonicalBytes(v any) ([]byte, error) {
	return json.Marshal(v)
}

func DigestJSONBytes(body []byte) (string, error) {
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func CloneJSON[T any](in T) (T, error) {
	body, err := json.Marshal(in)
	if err != nil {
		var zero T
		return zero, err
	}
	var out T
	if err := json.Unmarshal(body, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}
