# Niri Staging R4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the mandatory Niri 26.04 contract validate the exact production staged config tree, fail for the wrong workspace mapping, and pass for the correct mapping without weakening path-swap or symlink security.

**Architecture:** Begin with one temporary in-derivation diagnostic validator that runs direct Niri and `NiriValidator` before staging cleanup on the same bytes and pathname, while the whole test is file/process traced. Only after the trace identifies the root cause, add one behavior-level RED test and the smallest correction at that boundary; if Niri cannot safely consume an ephemeral tree, replace it with a private persistent transaction-state staging area with journaled reconciliation rather than descriptor tricks.

**Tech Stack:** Rust 2021, Niri 26.04 from pinned nixpkgs, Nix `buildRustPackage`, libc descriptor APIs, Cargo tests, strace when the build sandbox permits it, Docker-hosted Nix.

**Spec:** R4 task payload assigned 2026-08-24 for `/home/lazy/Projects/sleepy/.worktrees/desktop-m1/components/sleepy-session-aggregate`.

## Global Constraints

- Do not make another production fix until the diagnostic proves the root cause.
- Compare direct Niri and production `NiriValidator` on the byte-identical same path and build UID.
- Record PID/PPID, UID/GID, mount namespace, current executable, exact argv, staging pathname/inode, and existence before and after both calls.
- Distinguish cleanup lifetime, bounded-runner timeout/kill behavior, mount namespace, test-executable behavior, and event-stream influence with evidence.
- Preserve descriptor-relative/no-follow source copying and symlink/path-swap rejection.
- The mandatory exact production staged-tree check must fail for `workspace.next => focus-workspace-up` and pass for `workspace.next => focus-workspace-down`; no skip, fallback, direct-render-only check, or silent bypass is allowed.
- Final verification includes fmt, clippy with warnings denied, complete debug and release tests, release build, exact Docker Niri check, and Docker full flake check.
- Commit and push only the coherent final implementation to `feat/desktop-m2-session`.

---

### Task 1: Same-path in-derivation root-cause diagnostic

**Files:**
- Modify temporarily: `tests/bindings.rs`
- Modify temporarily: `flake.nix`

**Interfaces:**
- Consumes: `BindingValidator::validate(staged_root, staged_config)`, `NiriValidator`, and `apply_active_bindings`.
- Produces: one diagnostic log and syscall trace comparing direct and production validation before the staging tree can be removed.

- [ ] **Step 1: Add the diagnostic validator**

Implement a test-only validator that receives the real production staging pathname, logs process/mount/path metadata, runs exact argv `[niri, validate, --config, staged_config]` directly, invokes `NiriValidator::validate` with the same executable and pathname, runs the direct command again, and records all three results without returning an early validation error.

- [ ] **Step 2: Trace the exact derivation**

Run the ignored contract test under `strace -ff -yy -s 512 -e trace=%file,%process` when available in `checks.x86_64-linux.niri-bindings`, print the trace on failure, and build the check in the existing Nix Docker environment.

- [ ] **Step 3: Classify the root cause**

Compare timestamps, path inode/existence, process and namespace identity, exact exec argv, child exit timing, and every failing `openat`/`statx`. State one falsifiable root-cause conclusion and discard hypotheses contradicted by the trace. Do not proceed until direct and production behavior differ at a specific observed boundary or both demonstrate an Niri API limitation.

---

### Task 2: Root-cause regression and minimal correction

**Files:**
- Modify: exact production file identified by Task 1
- Modify: `tests/bindings.rs`
- Modify: `tests/flake.rs` only if the final flake interface changes
- Modify: `flake.nix`

**Interfaces:**
- Consumes: the proven failing boundary from Task 1.
- Produces: a mandatory Niri 26.04 check that executes `apply_active_bindings` with the production `NiriValidator` against the complete staged fixture tree.

- [ ] **Step 1: Write the behavior RED**

Replace temporary diagnostics with the narrowest persistent test that catches the proven bug. Name the production mutation it catches and use the real validator/staging path. Run it before production edits and record the expected failure.

- [ ] **Step 2: Implement one correction**

Change only the proven boundary. If the trace proves ephemeral-tree validation is fundamentally unsafe, stage beneath a private `0700` XDG transaction-state directory, retain descriptor authority, publish exact UUID entries with no-follow exclusive writes, persist enough journal state to reconcile abandoned staging entries, and remove only validated transaction-owned names after validator completion or startup reconciliation.

- [ ] **Step 3: Verify security regressions**

Run the existing source-entry swap, writable-path swap, malicious symlink, owner/mode, cleanup, crash, and reconciliation tests. Add only the path-swap/abandoned-stage cases required by the chosen architecture.

---

### Task 3: Mapping mutation proof and full verification

**Files:**
- Modify temporarily for mutation proof: `src/bindings/actions.rs`
- Restore before commit: `src/bindings/actions.rs`
- Modify: Task 2 final files only

**Interfaces:**
- Consumes: final mandatory check.
- Produces: explicit RED on the bad workspace mapping and GREEN on the correct mapping.

- [ ] **Step 1: Prove mapping RED**

Temporarily change only `workspace.next` from `focus-workspace-down` to `focus-workspace-up`, run `checks.x86_64-linux.niri-bindings`, and record the exact contract's semantic assertion failure after production Niri validation. Restore the correct mapping with `apply_patch`.

- [ ] **Step 2: Prove exact-check GREEN**

Run the same check unchanged with `workspace.next => focus-workspace-down` and verify that the production staged-tree path is exercised and succeeds.

- [ ] **Step 3: Run complete verification**

Run `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`, `cargo test --release --all-targets`, `cargo build --release --all-targets`, `git diff --check`, the exact Docker Niri check, and Docker `nix flake check -L` for the full flake.

- [ ] **Step 4: Commit and push**

Review the complete diff, ensure temporary diagnostic and mutation code is absent, commit the coherent R4 fix, push `feat/desktop-m2-session`, and report the SHA plus exact RED/GREEN and verification evidence to the parent and downstream desktop owner.
