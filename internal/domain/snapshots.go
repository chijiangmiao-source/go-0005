package domain

import "errors"

var ErrSnapshotImmutable = errors.New("snapshot is immutable once registered")

func (s OrbitSnapshot) Normalize() OrbitSnapshot {
	s.Valid = s.Valid.Normalize()
	for i := range s.Envelope {
		s.Envelope[i].At = NormalizeTime(s.Envelope[i].At)
	}
	return s
}

func (s SeaSnapshot) Normalize() SeaSnapshot {
	s.Valid = s.Valid.Normalize()
	for i := range s.Samples {
		s.Samples[i].At = NormalizeTime(s.Samples[i].At)
	}
	return s
}

func (s OrbitSnapshot) ExpectedDigest() (string, error) {
	s = s.Normalize()
	s.Digest = ""
	return DigestCanonical(s)
}

func (s SeaSnapshot) ExpectedDigest() (string, error) {
	s = s.Normalize()
	s.Digest = ""
	return DigestCanonical(s)
}

func (s OrbitSnapshot) WithVerifiedDigest() (OrbitSnapshot, error) {
	s = s.Normalize()
	digest, err := s.ExpectedDigest()
	if err != nil {
		return s, err
	}
	if s.Digest != "" && s.Digest != digest {
		return s, ErrSnapshotImmutable
	}
	s.Digest = digest
	return s, nil
}

func (s SeaSnapshot) WithVerifiedDigest() (SeaSnapshot, error) {
	s = s.Normalize()
	digest, err := s.ExpectedDigest()
	if err != nil {
		return s, err
	}
	if s.Digest != "" && s.Digest != digest {
		return s, ErrSnapshotImmutable
	}
	s.Digest = digest
	return s, nil
}

func (s OrbitSnapshot) Covers(r TimeRange) bool {
	return s.Valid.Normalize().ContainsWindow(r.Normalize())
}

func (s SeaSnapshot) Covers(r TimeRange) bool {
	return s.Valid.Normalize().ContainsWindow(r.Normalize())
}
