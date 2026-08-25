# Task 3 + Task 4 Aggregate Integration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the complete accepted binding transaction implementation and the complete accepted system/session adapter implementation in one ancestry-preserving aggregate branch and binary.

**Architecture:** Merge Task 4 commit `e5f4eef10f42e186a0f04bfa07c1cba005c6f757` non-fast-forward into Task 3 `e288ba3037548f808fb98f34efb7b16524870cf2`. Resolve only overlap points by composing match arms, module exports, structured errors, imports, helpers, and tests from both sides; retain all non-conflicting ancestry unchanged.

**Tech Stack:** Rust 2021, Cargo, Git non-ff merge, sleepy-sdk exact revision `5dc792faea9d743fabbb576ae1b25ed7e1f729f9`.

**Spec:** Aggregate Task3+Task4 payload received 2026-08-24.

## Global Constraints

- Preserve both accepted histories; do not reimplement either task.
- Record actual conflict and compile/test RED before resolution.
- CLI must expose bindings plus system show/set and session perform in one binary.
- Structured error variants must remain distinct; SDK pin must remain exact.
- Commit aggregate resolution under `feat:` or `fix:`, keep clean, no push.

---

### Task 1: Merge and conflict composition

**Files:**
- Modify conflicts: `src/cli.rs`, `src/lib.rs`, `src/store/error.rs`, `tests/cli.rs`
- Add from Task 4: `src/system/**`, `tests/system.rs`, `tests/fixtures/system/**`

**Interfaces:**
- Consumes: Task 3 `bindings`, `BindingError`, binding CLI grammar; Task 4 `system`, `SystemError`, system/session CLI grammar.
- Produces: one `run` dispatch and one JSON envelope retaining every accepted command.

- [ ] Run `git merge --no-ff --no-commit e5f4eef...` and record unmerged paths as conflict RED.
- [ ] Compare base/ours/theirs for each conflict and compose both sides with no deleted commands, error codes, exports, helpers, or tests.
- [ ] Run fmt and compile/tests; record any aggregate RED and fix only integration defects.

### Task 2: Aggregate contract and verification

**Files:**
- Test: `tests/cli.rs`
- Report: root SDD `task-3-4-aggregate-report.md`

**Interfaces:**
- Consumes: merged `sleepyctl` binary.
- Produces: proof that binding apply and system show/set/session perform command families coexist.

- [ ] Inspect existing merged CLI tests; if they do not prove coexistence in one binary, add a focused failing aggregate test before dispatch changes.
- [ ] Run SDK pin test plus focused bindings/system/session CLI tests.
- [ ] Run fmt check, clippy all targets/features with warnings denied, debug/release all-target tests, release all-target build, diff checks, and clean status.
- [ ] Commit the merge resolution, append exact conflict/RED/GREEN evidence, and report final SHA without pushing.
