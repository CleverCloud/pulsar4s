# Spec: Replace custom CompletableFuture conversion with `Async[F].fromCompletableFuture`

**Issue:** [#485](https://github.com/CleverCloud/pulsar4s/issues/485)

## Context

The Cats Effect async handler (`CatsAsyncHandler.scala`) uses a custom `CompletableFutureConverters` to convert `CompletableFuture[T]` to `F[T]`. Cats Effect 3 provides a built-in `Async[F].fromCompletableFuture` that handles this conversion with proper cancellation semantics.

## Current Implementation

In `pulsar4s-cats-effect/src/main/scala/com/sksamuel/pulsar4s/cats/CatsAsyncHandler.scala`, the `CompletableOps.liftF` method:

1. Checks if the future is already done (fast path with `f.get()`)
2. Otherwise registers a callback via `Async[F].async_`
3. Does **not** propagate fiber cancellation back to the `CompletableFuture`

## Problem

- `Async[F].async_` does not support a cancellation finalizer. If the fiber is canceled, the underlying `CompletableFuture` continues running.
- `Async[F].fromCompletableFuture` (available since Cats Effect 3.x) uses `Async[F].async` with a finalizer that calls `CompletableFuture.cancel()`, giving better cancellation semantics.

## Changes Required

### 1. Replace `CompletableFutureConverters` with `Async[F].fromCompletableFuture`

**File:** `pulsar4s-cats-effect/src/main/scala/com/sksamuel/pulsar4s/cats/CatsAsyncHandler.scala`

- Remove the `CompletableFutureConverters` object (lines 26-63)
- Replace all `.liftF` calls with `Async[F].fromCompletableFuture(Async[F].delay(expr))` or an equivalent helper

For example, change:
```scala
Async[F].delay { builder.createAsync() }.liftF.map(new DefaultProducer(_))
```
to:
```scala
Async[F].fromCompletableFuture(Async[F].delay(builder.createAsync())).map(new DefaultProducer(_))
```

### 2. Handle `CompletableFuture[Void]` cases

`fromCompletableFuture` returns `F[T]`. For `CompletableFuture[Void]`, the result is `null`. The current `.void` calls already discard the result, so this should work as-is. Verify that `fromCompletableFuture` doesn't fail on a `null` result value.

### 3. Update tests

- Verify existing tests still pass (cancellation behavior may change subtly)
- Consider adding a test that canceling a fiber also cancels the underlying `CompletableFuture`

## Acceptance Criteria

- [ ] `CompletableFutureConverters` is removed
- [ ] All `CompletableFuture` conversions use `Async[F].fromCompletableFuture`
- [ ] Existing tests pass
- [ ] Fiber cancellation propagates to underlying `CompletableFuture`
