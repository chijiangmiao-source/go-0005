const output = document.querySelector("#output");
const batchInput = document.querySelector("#batch-id");
const eventSeqInput = document.querySelector("#event-seq");
const refreshButton = document.querySelector("#refresh");

const print = (value) => {
  if (output) {
    output.textContent = JSON.stringify(value, null, 2);
  }
};

document.querySelector("#check")?.addEventListener("click", async () => {
  const res = await fetch("/health/ready");
  print({ status: res.status, body: await res.json() });
});

document.querySelector("#resources")?.addEventListener("click", async () => {
  const res = await fetch("/api/v1/resources");
  print({ status: res.status, body: await res.json() });
});

document.querySelector("#sample")?.addEventListener("click", async () => {
  const now = new Date("2026-01-01T00:00:00Z");
  const end = new Date(now.getTime() + 10 * 60 * 1000);
  const orbit = {
    source: { source: "public-tle", version: "browser", source_time: "2025-12-31T23:00:00Z" },
    valid: { start: "2025-12-31T23:59:00Z", end: "2026-01-01T00:20:00Z" },
    max_angular_rate_deg_per_second: 1,
    envelope: [
      { at: now.toISOString(), roll_min_deg: -2, roll_max_deg: 2, pitch_min_deg: -2, pitch_max_deg: 2 },
      { at: end.toISOString(), roll_min_deg: -2, roll_max_deg: 2, pitch_min_deg: -2, pitch_max_deg: 2 }
    ]
  };
  const sea = {
    source: { source: "public-buoy", version: "browser", source_time: "2025-12-31T23:00:00Z" },
    valid: { start: "2025-12-31T23:59:00Z", end: "2026-01-01T00:20:00Z" },
    max_sample_gap: 600000000000,
    samples: [
      { at: now.toISOString(), significant_wave_height_m: 2, wind_speed_m_s: 9, heave_m: 1 },
      { at: end.toISOString(), significant_wave_height_m: 2, wind_speed_m_s: 9, heave_m: 1 }
    ]
  };
  const orbitRes = await fetch("/api/v1/orbit-snapshots", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(orbit) });
  const orbitBody = await orbitRes.json();
  const seaRes = await fetch("/api/v1/sea-snapshots", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(sea) });
  const seaBody = await seaRes.json();
  const body = {
    id: document.querySelector("#application-id")?.value || "browser-application-1",
    orbit_snapshot_id: orbitBody.id,
    sea_snapshot_id: seaBody.id,
    window: { start: now.toISOString(), end: end.toISOString() },
    posture: [
      { at: now.toISOString(), roll_deg: 0, pitch_deg: 0 },
      { at: end.toISOString(), roll_deg: 1, pitch_deg: 1 }
    ],
    max_angular_rate_deg_per_second: 1,
    sea_limits: { max_wave_height_m: 2, max_wind_speed_m_s: 9, max_heave_m: 1 },
    resources: [{ resource_id: "antenna", quantity: 1, critical: true, timeout: 30000000000 }]
  };
  const res = await fetch("/api/v1/applications", {
    method: "POST",
    headers: { "content-type": "application/json", "Idempotency-Key": "browser-sample" },
    body: JSON.stringify(body)
  });
  const payload = await res.json();
  if (payload.batch_id && batchInput && eventSeqInput && refreshButton) {
    batchInput.value = payload.batch_id;
    eventSeqInput.value = String(payload.event_seq || 0);
    refreshButton.disabled = false;
  }
  print({ snapshots: { orbit: orbitBody, sea: seaBody }, application: { status: res.status, body: payload } });
});

refreshButton?.addEventListener("click", async () => {
  if (!batchInput?.value) return;
  const res = await fetch(`/api/v1/batches/${batchInput.value}`);
  const body = await res.json();
  if (eventSeqInput && Array.isArray(body.events) && body.events.length > 0) {
    eventSeqInput.value = String(body.events[body.events.length - 1].aggregate_seq);
  }
  print({ status: res.status, body });
});
