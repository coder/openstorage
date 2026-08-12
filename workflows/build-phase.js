// Durable conductor: builds ONE phase of the open-source code.storage backend.
// The human gate lives BETWEEN runs: each run ends with a go/no-go gate report;
// the operator reviews it and launches the next phase (optionally with a `brief`).
//
// Usage:
//   workflow_run({ script_path: "./workflows/build-phase.js",
//                  args: { phase: "p0-spike", brief: "optional operator guidance" } })

const s = mux.schema;

export const meta = {
  description:
    "Build one roadmap phase: plan -> slice -> foundation -> parallel implement -> integrate -> verify loop -> parallel review -> fix -> dogfood -> document -> gate report",
  argsSchema: s.object(
    {
      phase: s.enum([
        "p0-spike",
        "p1-skeleton-auth",
        "p2-git-http",
        "p3-read-api",
        "p4-write-api",
        "p5-eventing-sync",
        "p6-lfs-polish",
        "custom",
      ]),
      // Required when phase = "custom": a one-off phase spec with the same
      // shape as a PHASES entry, plus an id used for evidence paths/reports.
      spec: s.optional(
        s.object(
          {
            id: s.string(),
            title: s.string(),
            specRefs: s.string(),
            deliverables: s.string(),
            acceptance: s.array(s.string(), { minItems: 1 }),
            dogfood: s.string(),
          },
          { additionalProperties: false }
        )
      ),
      brief: s.optional(s.string()),
      maxFixRounds: s.optional(s.number()),
      // REQUIRED (fail fast): true in worktree-isolated runtimes (patch fan-in via git am),
      // false ONLY in shared-working-tree runtimes. Omitting this in a worktree runtime
      // silently dropped an entire phase's implementation (p6, 2026-08-12).
      isolated: s.boolean(),
    },
    { additionalProperties: false }
  ),
};

const SPEC_POINTER =
  "Authoritative specs live in docs/ (01-domain-model .. 07-implementation-roadmap, README.md) " +
  "and docs/upstream/openapi.yaml. Architecture: stateless Go nodes; Postgres = ref authority " +
  "(CAS) + metadata; S3/MinIO = immutable packs via gocloud.dev/blob; local disk = rebuildable LRU cache.";

const PHASES = {
  "p0-spike": {
    title: "De-risk spike: git push -> S3 -> PG-CAS -> clone from a second node",
    specRefs: "docs/05-git-protocol-and-namespaces.md, docs/07-implementation-roadmap.md",
    deliverables:
      "Working spike (throwaway quality allowed, but tested): git receive-pack behind JWT basic auth (user 't'); " +
      "hook routes every ref update through a Postgres CAS transaction; packs uploaded to MinIO BEFORE the ref flip; " +
      "a second process with an empty cache dir serves a clone rehydrated from MinIO+PG.",
    acceptance: [
      "git push to node A succeeds; refs live in Postgres; pack objects exist in MinIO",
      "git clone from node B (fresh cache dir) yields identical HEAD",
      "two concurrent pushes to one branch: exactly one wins CAS, loser gets a git-native rejection",
      "simulated crash between S3 upload and PG commit leaves only orphaned packs, never a broken ref (test proves it)",
      "force-push under a no-force-push JWT ref policy is rejected per-ref",
    ],
    dogfood:
      "Terminal transcript of: compose up (PG+MinIO), real `git push` to node A, `git clone` from node B, " +
      "rejected force-push under a no-force-push JWT.",
  },
  "p1-skeleton-auth": {
    title: "Skeleton + auth: org registry, JWT middleware, repo CRUD, error envelope",
    specRefs: "docs/02-authentication.md, docs/03-http-api.md (repo lifecycle), docs/01-domain-model.md",
    deliverables:
      "Go service skeleton: BlobStore/RefStore interfaces; JWT middleware (ES256/RS256/EdDSA + PS256/ES384/ES512, " +
      "iss/repo/scopes/exp checks); {error} envelope; POST /repos, GET /repo, GET /repos (cursor pagination, q filter), " +
      "DELETE /repos/delete (soft delete, 404/409 semantics); single-org self-host mode; compose quickstart.",
    acceptance: [
      "repo CRUD passes against PG+MinIO testcontainers, incl. 401/403/404/409 matrix from docs/03",
      "vendor Go SDK (apiBaseUrl override) performs createRepo/findOne/listRepos/deleteRepo unmodified",
      "JWT negative tests: wrong alg, expired, wrong iss, wrong repo claim, missing scope all rejected correctly",
    ],
    dogfood: "Transcript: compose up, mint JWT with a local key, drive repo CRUD via curl AND via the vendor Go SDK.",
  },
  "p2-git-http": {
    title: "Git smart HTTP: upload-pack/receive-pack, namespaces, policy hooks",
    specRefs: "docs/05-git-protocol-and-namespaces.md, docs/02-authentication.md (ref policies)",
    deliverables:
      "Smart HTTP on the storage host paths: info/refs + upload-pack (git:read), receive-pack (git:write) with " +
      "PG-CAS ref authority and S3-before-ref-flip ordering; +ephemeral/+import namespace routing; " +
      "no-push/no-force-push/verify-sig enforcement, first-match-wins.",
    acceptance: [
      "clone/push/fetch round-trip with the real git CLI on all three namespaces",
      "two-replica test: push to node A, clone from node B with cold cache",
      "ref-policy matrix tests pass; ephemeral refs invisible from persistent remotes",
    ],
    dogfood: "Transcript: two service replicas, push A/clone B, ephemeral push + promotion prep, policy rejections.",
  },
  "p3-read-api": {
    title: "Read API: files/branches/tags/commits/diff/blame/grep/archive",
    specRefs: "docs/03-http-api.md, docs/01-domain-model.md (invariants 2,3,9,10)",
    deliverables:
      "All git:read endpoints incl. conditional/range file streaming (X-Blob-Sha, ETag, 206/304/412/416), " +
      "unborn-branch 404s, cursor pagination everywhere, camelCase quirks on GET /repos/diff.",
    acceptance: [
      "endpoint tests match docs/03 status-code contract and response shapes",
      "vendor SDK read methods pass against the service",
      "openapi.yaml schema validation on read endpoints (schemathesis or equivalent)",
    ],
    dogfood: "Transcript: seed a repo via git push, then drive log/show/diff/blame/grep/ls via the vendor SDK.",
  },
  "p4-write-api": {
    title: "Write API: NDJSON commit-pack/diff-commit, merge, branches, tags, notes",
    specRefs: "docs/04-streaming-commit-protocols.md, docs/03-http-api.md, docs/01-domain-model.md (invariants 1-6)",
    deliverables:
      "Streaming NDJSON ingestion (incremental parse, 4MiB chunks, chunked bodies without Content-Length); " +
      "result envelope on the documented status list; CAS everywhere; merge + read-only preview; " +
      "default-branch delete protection; notes with expected_ref_sha.",
    acceptance: [
      "commit-pack/diff-commit/restore-commit pass CAS + conflict envelope tests (409/412, zero-SHA root commits)",
      "vendor full-workflow.js integration test passes unmodified",
      "just-code-storage git command examples work against the service",
    ],
    dogfood: "Transcript: run vendor full-workflow.js and a just-code-storage init/add/commit/push/log/merge session.",
  },
  "p5-eventing-sync": {
    title: "Eventing & sync: webhooks, upstream mirrors, forking",
    specRefs: "docs/06-webhooks.md, docs/03-http-api.md (git-credentials, pull-upstream), docs/01 (invariants 7,11,12)",
    deliverables:
      "River-backed webhook delivery (HMAC t=..,sha256=.. signing, re-sign on retry); base_repo sync workers; " +
      "pull-upstream 202; git-credentials CRUD; provider:code forking; ephemeral updates emit no push events.",
    acceptance: [
      "webhook receiver verifies signatures with all three vendor SDK verifier helpers",
      "sync lifecycle emits started/succeeded/failed with documented error strings",
      "fork of a code.storage repo at ref/sha produces an independent repo",
    ],
    dogfood: "Transcript: local webhook receiver logging verified events during pushes and a sync cycle.",
  },
  "p6-lfs-polish": {
    title: "LFS + polish: batch API, named-repo routes, repo-urls, rate limits",
    specRefs: "docs/05-git-protocol-and-namespaces.md (LFS), docs/03-http-api.md, docs/README.md (path styles)",
    deliverables:
      "LFS batch endpoint with presigned S3 transfers; /api/repos/{repo_name}/... routing aliases; " +
      "GET /api/repo-urls/{repo_id}; basic rate limiting; two-replica compose profile in the quickstart.",
    acceptance: [
      "git-lfs CLI round-trip (track/push/clone) against the service",
      "named-repo aliases return identical bodies to v1 for a sampled endpoint matrix",
    ],
    dogfood: "Transcript: git-lfs push/clone of a binary file; alias-vs-v1 curl diff.",
  },
};

const REVIEW_LANES = [
  {
    name: "correctness-concurrency",
    focus:
      "CAS/transaction ordering, S3-before-PG-ref-flip crash consistency, races, error handling, defensive asserts on function args/returns",
  },
  {
    name: "wire-conformance",
    focus:
      "exact conformance to docs/02..04 and docs/upstream/openapi.yaml: paths, param names/casing, status codes, envelopes, headers",
  },
  {
    name: "security",
    focus:
      "JWT verification (alg confusion, expiry, iss/repo/scope checks), ref-policy enforcement on every write path, path traversal, secret handling",
  },
  {
    name: "tests-and-slop",
    focus:
      "test quality and coverage of the new surface; slop: dead code, speculative abstraction, inconsistent style, unnecessary comments",
  },
];

function sliceSchema() {
  return s.object(
    {
      foundation: s.nullable(s.string()),
      slices: s.array(
        s.object(
          {
            name: s.string(),
            description: s.string(),
            ownedPaths: s.array(s.string()),
          },
          { additionalProperties: false }
        ),
        { minItems: 1, maxItems: 4 }
      ),
      rationale: s.string(),
    },
    { additionalProperties: false }
  );
}

function sanitizeName(name) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

function verifySchema() {
  return s.object(
    {
      passed: s.boolean(),
      commandsRun: s.array(s.string()),
      failures: s.array(
        s.object({ command: s.string(), summary: s.string() }, { additionalProperties: false })
      ),
      notes: s.optional(s.string()),
    },
    { additionalProperties: false }
  );
}

function reviewSchema() {
  return s.object(
    {
      issues: s.array(
        s.object(
          {
            title: s.string(),
            severity: s.enum(["P0", "P1", "P2", "P3"]),
            filePaths: s.array(s.string()),
            evidence: s.string(),
            suggestedFix: s.optional(s.string()),
          },
          { additionalProperties: false }
        )
      ),
    },
    { additionalProperties: false }
  );
}

function recheckSchema() {
  return s.object(
    {
      resolved: s.array(s.string()),
      unresolved: s.array(
        s.object({ title: s.string(), reason: s.string() }, { additionalProperties: false })
      ),
    },
    { additionalProperties: false }
  );
}

function dogfoodSchema() {
  return s.object(
    {
      passed: s.boolean(),
      evidencePaths: s.array(s.string()),
      acceptanceResults: s.array(
        s.object(
          { criterion: s.string(), passed: s.boolean(), evidence: s.string() },
          { additionalProperties: false }
        )
      ),
      notes: s.optional(s.string()),
    },
    { additionalProperties: false }
  );
}

export default function workflow({ args, phase, log, agent, parallel, applyPatch }) {
  const spec = args.phase === "custom" ? args.spec : PHASES[args.phase];
  if (args.phase === "custom" && !spec) throw new Error("phase 'custom' requires args.spec");
  const phaseId = args.phase === "custom" ? args.spec.id : args.phase;
  const maxFix = Math.max(1, Math.min(args.maxFixRounds ?? 2, 4));
  // isolated=false: local/shared-working-tree runtime. Child agents write directly
  // into this checkout, so patch fan-in is a no-op (skipped) and write fan-out is serialized.
  // isolated=true: worktree-isolated workspace; only COMMITTED child work survives patch fan-in.
  // Explicitly required (no default): a wrong value here silently discards all child work.
  if (typeof args.isolated !== "boolean") {
    throw new Error(
      "args.isolated must be explicitly true (worktree-isolated runtime, patch fan-in) or " +
        "false (shared working tree). No default: a wrong guess drops the whole implementation."
    );
  }
  const isolated = args.isolated;
  const applyChildPatch = (id, agentId) => {
    if (!isolated) return; // shared working tree: child changes are already on disk
    const res = applyPatch({ id, agentId });
    if (!res.success) log(`patch not applied cleanly: ${agentId}`, { status: res.status ?? res.error });
  };
  const operatorBrief = args.brief ? `\n\nOperator guidance for this phase:\n${args.brief}` : "";
  // In isolated mode, fan-in uses git-format-patch artifacts: only COMMITTED child work survives.
  const commitRule = isolated
    ? "\n\nIMPORTANT: You work in an isolated fork of the parent workspace; only COMMITTED changes are " +
      "integrated back (patch fan-in drops uncommitted files). Before finishing, commit ALL your changes " +
      "on the current branch with descriptive messages (git add -A && git commit). Never push."
    : "";

  // ---- Plan ----
  phase("plan", { phase: phaseId, title: spec.title });
  const plan = agent(
    `Plan this phase of the open-source code.storage backend (Go, Postgres, S3/MinIO).\n\n` +
      `Phase: ${spec.title}\nSpec references (read them): ${spec.specRefs}\n${SPEC_POINTER}\n\n` +
      `Deliverables: ${spec.deliverables}\n\nAcceptance criteria the plan MUST cover:\n- ${spec.acceptance.join("\n- ")}\n\n` +
      `The plan must include a dedicated dogfooding section (environment setup + exact commands + evidence capture): ${spec.dogfood}\n` +
      `Survey the existing code in the workspace first and build on it; keep the plan minimal and reviewable.${operatorBrief}`,
    { id: "plan", agentId: "plan", title: `Plan ${phaseId}` }
  );

  // ---- Slice the plan for parallel implementation ----
  phase("slice-plan");
  const slicing = agent(
    `Analyze this implementation plan and partition it into 1-4 INDEPENDENT slices for parallel implementation ` +
      `by separate exec agents.\n\nRules:\n` +
      `- "foundation": shared scaffolding that must land FIRST (interfaces, types, DB migrations, module wiring, ` +
      `shared test helpers). Null if none needed. Slice agents will treat foundation code as frozen contracts.\n` +
      `- Each slice must own a DISJOINT set of files/directories (ownedPaths). No two slices may write the same file.\n` +
      `- Slices must be independently implementable and testable once the foundation exists.\n` +
      `- If the plan is inherently serial (one vertical thread, heavy interdependence), return exactly ONE slice — ` +
      `do not force parallelism. Explain in "rationale".\n\nPlan:\n${plan.reportMarkdown}`,
    { id: "slice-plan", agentId: "explore", schema: sliceSchema(), title: "Slice plan for parallel implementation" }
  );
  const sliceIds = slicing.slices.map((sl, i) => `s${i + 1}-${sanitizeName(sl.name)}`);
  log("Slicing decision", { foundation: Boolean(slicing.foundation), slices: sliceIds, rationale: slicing.rationale });

  // ---- Foundation (serial, contract-first) ----
  let foundationSummary = "";
  if (slicing.foundation) {
    phase("implement-foundation");
    foundationSummary = agent(
      `Implement ONLY the shared foundation for this plan: ${slicing.foundation}\n\n` +
        `This is contract-first work: interfaces, types, migrations, wiring, shared test helpers. Parallel slice ` +
        `agents will build on top of it and treat it as frozen — make signatures deliberate. It must compile and ` +
        `pass go vet. Do NOT implement the slices themselves.\n\nFull plan for context:\n${plan.reportMarkdown}${operatorBrief}${commitRule}`,
      {
        id: "implement-foundation",
        title: "Implement foundation",
        timeout: { softMs: 45 * 60_000, graceMs: 5 * 60_000, finalInstructions: "Ensure what exists compiles; list unfinished contracts explicitly." },
      }
    );
    applyChildPatch("apply-foundation", "implement-foundation");
  }

  // ---- Parallel slice implementation (fan-out) ----
  phase("implement-slices", { slices: sliceIds });
  const sliceSummaries = parallel(
    slicing.slices.map((sl, i) => () =>
      agent(
        `Implement ONE slice of a larger plan. ` +
          (isolated
            ? `Sibling agents are implementing other slices CONCURRENTLY in isolated worktrees.\n\n`
            : `Slices run SEQUENTIALLY in a SHARED working tree; earlier slices' changes are already present. ` +
              `Stay on the current branch — never git reset, rebase, or switch branches; keep scratch files in /tmp.\n\n`) +
          `Your slice: ${sl.name}\n${sl.description}\n\nFile ownership (STRICT): you may create/modify ONLY files ` +
          `under: ${sl.ownedPaths.join(", ")}. Foundation code and files owned by other slices are read-only frozen ` +
          `contracts — if your slice seems to require changing them, do NOT change them; implement what you can and ` +
          `report the constraint conflict explicitly in your final summary.\n\n` +
          `Write tests alongside code within your owned paths. Make minimal, correct, reviewable changes with ` +
          `defensive asserts.\n\nFull plan for context:\n${plan.reportMarkdown}${operatorBrief}${commitRule}`,
        {
          id: `implement-slice-${sliceIds[i]}`,
          title: `Implement slice: ${sl.name}`,
          timeout: { softMs: 75 * 60_000, graceMs: 5 * 60_000, finalInstructions: "Ensure owned files compile in isolation where possible; list remaining work explicitly." },
        }
      )
    ),
    { maxParallel: isolated ? 4 : 1 }
  );

  // ---- Fan-in: apply slice patches sequentially, then integrate ----
  phase("integrate-slices", { slices: sliceIds });
  for (let i = 0; i < sliceIds.length; i++) {
    applyChildPatch(`apply-slice-${sliceIds[i]}`, `implement-slice-${sliceIds[i]}`);
  }
  const implementSummary = agent(
    `Integrate the just-applied parallel implementation slices into a coherent whole. Fix compile errors, import/` +
      `signature drift between slices, duplicate helpers, and any constraint conflicts the slice agents reported. ` +
      `You may touch any file. Do NOT add new features beyond the plan. Finish with the full build green ` +
      `(go build ./... && go vet ./...).\n\nFoundation summary:\n${foundationSummary || "(none)"}\n\nSlice summaries:\n` +
      sliceSummaries.map((summary, i) => `--- ${slicing.slices[i].name} ---\n${summary}`).join("\n\n") +
      commitRule,
    {
      id: "integrate-slices",
      title: "Integrate slices",
      timeout: { softMs: 45 * 60_000, graceMs: 5 * 60_000, finalInstructions: "Prioritize a green build; list any unresolved integration issues explicitly." },
    }
  );
  applyChildPatch("apply-integrate", "integrate-slices");

  // ---- Verify loop (verify and fix are separate agents; bounded rounds) ----
  let verify = null;
  for (let round = 0; round <= maxFix; round++) {
    phase(round === 0 ? "verify" : `verify-round-${round}`);
    verify = agent(
      `Run the full validation suite for this repo and report results honestly. Do NOT fix anything.\n` +
        `Typical commands: go build ./..., go vet ./..., golangci-lint run (if configured), go test ./... ` +
        `(integration tests may need docker for testcontainers). Include every command you ran.`,
      { id: `verify-${round}`, schema: verifySchema(), title: `Verify round ${round}` }
    );
    if (verify.passed) break;
    if (round === maxFix) break;
    phase(`fix-round-${round + 1}`, { failures: verify.failures.length });
    agent(
      `Fix these validation failures with minimal changes. Do not suppress or skip tests; fix root causes.\n\n` +
        JSON.stringify(verify.failures, null, 2) +
        commitRule,
      { id: `fix-${round}`, title: `Fix round ${round + 1}` }
    );
    applyChildPatch(`apply-fix-${round}`, `fix-${round}`);
  }
  if (!verify.passed) {
    return gateReport(agent, phase, phaseId, spec, {
      go: false,
      reason: "validation still failing after fix rounds",
      verify,
      reviews: [],
      recheck: null,
      dogfood: null,
    });
  }

  // ---- Parallel code review ----
  phase("code-review", { lanes: REVIEW_LANES.map((l) => l.name) });
  const reviews = parallel(
    REVIEW_LANES.map(
      (lane) => () =>
        agent(
          `Adversarial code review of the changes for phase "${spec.title}". Focus: ${lane.focus}.\n` +
            `Review the current branch's diff against its merge base; read spec docs (${spec.specRefs}) as the contract. ` +
            `Report only real issues with file:line evidence; severity P0 (broken/exploitable) to P3 (nit).`,
          { id: `review-${lane.name}`, agentId: "explore", schema: reviewSchema(), title: `Review: ${lane.name}` }
        )
    ),
    { maxParallel: 4 }
  );
  const allIssues = reviews.flatMap((r) => r.issues);
  const blocking = allIssues.filter((i) => i.severity === "P0" || i.severity === "P1");

  // ---- Fix blocking findings, re-verify, adversarial re-check ----
  let recheck = null;
  if (blocking.length > 0) {
    phase("fix-review-findings", { blocking: blocking.length });
    agent(
      `Fix these blocking code-review findings with minimal changes (root causes, not band-aids):\n\n` +
        JSON.stringify(blocking, null, 2) +
        commitRule,
      { id: "fix-findings", title: "Fix review findings" }
    );
    applyChildPatch("apply-fix-findings", "fix-findings");
    verify = agent(
      `Re-run the full validation suite after review fixes. Do NOT fix anything; report honestly with all commands run.`,
      { id: "verify-after-findings", schema: verifySchema(), title: "Re-verify after review fixes" }
    );
    recheck = agent(
      `Independently verify whether each of these previously-found blocking issues is now actually resolved ` +
        `in the current code. Be adversarial; do not take fix claims at face value.\n\n` +
        JSON.stringify(blocking, null, 2),
      { id: "recheck-findings", agentId: "explore", schema: recheckSchema(), title: "Re-check findings" }
    );
  }

  // ---- Dogfood ----
  phase("dogfood");
  const dogfood = agent(
    `Dogfood this phase end-to-end as a real user. ${spec.dogfood}\n` +
      `Bring up the environment (docker compose with Postgres + MinIO), run the real flows, and capture evidence: ` +
      `terminal recordings/transcripts via agent-tty (mise use npm:agent-tty) or util-linux script; save evidence under ` +
      `/tmp/codestorage-evidence/${phaseId}/ and report the paths. Evaluate each acceptance criterion:\n- ${spec.acceptance.join("\n- ")}\n` +
      `Report honestly; a criterion without captured evidence counts as failed.`,
    {
      id: "dogfood",
      schema: dogfoodSchema(),
      title: `Dogfood ${phaseId}`,
      timeout: { softMs: 45 * 60_000, graceMs: 5 * 60_000, finalInstructions: "Report per-criterion results with whatever evidence exists so far." },
    }
  );

  // ---- Document ----
  phase("document");
  agent(
    `Update project documentation for this phase: implementation notes / ADRs for non-obvious decisions ` +
      `(ref-authority-in-PG, S3-before-ref-flip, namespace mapping), README quickstart if setup changed, and ` +
      `docs/07-implementation-roadmap.md phase status. Match existing docs tone; no marketing fluff.\n\nWhat was built:\n${implementSummary}${commitRule}`,
    { id: "document", title: "Document phase" }
  );
  applyChildPatch("apply-document", "document");

  // ---- Gate ----
  const unresolvedBlocking = recheck ? recheck.unresolved.length : 0;
  const go = verify.passed && dogfood.passed && unresolvedBlocking === 0;
  return gateReport(agent, phase, phaseId, spec, {
    go,
    reason: go ? "all gates green" : "see failed gates",
    verify,
    reviews: allIssues,
    recheck,
    dogfood,
  });
}

function gateReport(agent, phase, phaseId, spec, state) {
  phase("gate-report", { go: state.go });
  const markdown = agent(
    `Write the phase gate report for "${spec.title}" (${phaseId}). Decision: ${state.go ? "GO" : "NO-GO"} (${state.reason}).\n` +
      `Summarize factually and densely: what was built, validation results, review findings (resolved + open, all severities), ` +
      `dogfood evidence (link the evidence paths), and exact open items blocking or deferred. ` +
      `End with a recommendation for the operator.\n\nData:\n` +
      JSON.stringify(
        { verify: state.verify, reviewIssues: state.reviews, recheck: state.recheck, dogfood: state.dogfood },
        null,
        2
      ),
    { id: "gate-report", title: "Gate report" }
  );
  return {
    reportMarkdown: markdown,
    structuredOutput: {
      phase: phaseId,
      gate: state.go ? "go" : "no-go",
      reason: state.reason,
      verifyPassed: state.verify ? state.verify.passed : false,
      dogfoodPassed: state.dogfood ? state.dogfood.passed : false,
      openBlocking: state.recheck ? state.recheck.unresolved : [],
      evidencePaths: state.dogfood ? state.dogfood.evidencePaths : [],
    },
  };
}
