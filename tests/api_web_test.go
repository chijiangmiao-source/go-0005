package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

func TestD10ApplicationIdempotencyByteIdenticalAndConflict(t *testing.T) {
	h := newPublicHarness(t)
	app, orbit, sea := publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	first, firstBody := postJSON(t, h.srv.URL+"/api/v1/applications", "same-key", app)
	second, secondBody := postJSON(t, h.srv.URL+"/api/v1/applications", "same-key", app)
	if first.StatusCode != http.StatusCreated || second.StatusCode != first.StatusCode || !bytes.Equal(firstBody, secondBody) {
		t.Fatalf("D10 expected byte-identical replay, first=%d second=%d", first.StatusCode, second.StatusCode)
	}
	app.Priority = 9
	conflict, _ := postJSON(t, h.srv.URL+"/api/v1/applications", "same-key", app)
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("expected idempotency conflict, got %d", conflict.StatusCode)
	}
}

func TestConstraintRejectAndResourceConflictHTTPMapping(t *testing.T) {
	h := newPublicHarness(t)
	app, orbit, sea := publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	app.Posture[1].RollDeg = 99
	rejected, _ := postJSON(t, h.srv.URL+"/api/v1/applications", "bad-constraint", app)
	if rejected.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 constraint rejection, got %d", rejected.StatusCode)
	}
	app, orbit, sea = publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	ok, _ := postJSON(t, h.srv.URL+"/api/v1/applications", "capacity-a", app)
	if ok.StatusCode != http.StatusCreated {
		t.Fatalf("expected first reservation created, got %d", ok.StatusCode)
	}
	conflict, _ := postJSON(t, h.srv.URL+"/api/v1/applications", "capacity-b", app)
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("expected resource conflict 409, got %d", conflict.StatusCode)
	}
}

func TestStaticPageAndResourcesEndpointLoad(t *testing.T) {
	h := newPublicHarness(t)
	res, err := http.Get(h.srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected root 200, got %d", res.StatusCode)
	}
	res, err = http.Get(h.srv.URL + "/api/v1/resources")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected resources 200, got %d", res.StatusCode)
	}
}

func TestFrontendBuildIsDeterministic(t *testing.T) {
	root := filepath.Dir(filepath.Dir(currentFile(t)))
	cmd := exec.Command("npm", "run", "build")
	cmd.Dir = filepath.Join(root, "web")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("npm run build failed: %v\n%s", err, out)
	}
	first := digestFile(t, filepath.Join(root, "internal", "api", "static", "app.js"))
	cmd = exec.Command("npm", "run", "build")
	cmd.Dir = filepath.Join(root, "web")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("second npm run build failed: %v\n%s", err, out)
	}
	second := digestFile(t, filepath.Join(root, "internal", "api", "static", "app.js"))
	if first != second {
		t.Fatalf("frontend build changed app digest: %s != %s", first, second)
	}
}

func decodeBatchID(t *testing.T, body []byte) string {
	t.Helper()
	var payload struct {
		BatchID string `json:"batch_id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.BatchID == "" {
		t.Fatalf("response does not contain batch_id: %s", body)
	}
	return payload.BatchID
}

func TestApplicationShapeValidationRequiresSnapshots(t *testing.T) {
	h := newPublicHarness(t)
	app, _, _ := publicFixture(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	res, _ := postJSON(t, h.srv.URL+"/api/v1/applications", "shape", app)
	if res.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected missing snapshot ids to fail with 422, got %d", res.StatusCode)
	}
}

func TestSnapshotDigestMismatchReturnsUnprocessable(t *testing.T) {
	h := newPublicHarness(t)
	_, orbit, _ := publicFixture(h.base)
	orbit.Digest = "not-the-canonical-digest"
	res, _ := postJSON(t, h.srv.URL+"/api/v1/orbit-snapshots", "", orbit)
	if res.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected snapshot digest mismatch 422, got %d", res.StatusCode)
	}
}

var _ = domain.StateReserved
