# MPMC Algos

## Ref

Rigtorp's turn-based MPMC queue

https://github.com/rigtorp/MPMCQueue

## Shared Data Structure (all strategies)

```
// 64-byte cacheline-aligned slot
struct Slot {
    u64 turn;        // 8B — version/turn number
    char data[56];   // 56B — payload
}

// Queue buffer layout on each server (total: 128 + 1024*64 = 65,664 bytes)
//   offset 0:   u64 tail          (64B cacheline, only first 8B used)
//   offset 64:  u64 head          (64B cacheline, only first 8B used)
//   offset 128: Slot queue[QUEUE_SIZE]  (1024 slots x 64B each)
//
// Initial state: tail=0, head=0, queue[i].turn=i for all i
```

## Simple RDMA MPMC (mpmc_simple)

### Producer push(val)

```
1. CLAIM POSITION
   pos = RDMA_FAA(server.tail, +1)          // signaled
   idx = pos & (QUEUE_SIZE - 1)

2. WAIT FOR TURN (spin)
   loop:
     old = RDMA_CAS(server.queue[idx].turn, pos, pos)   // signaled, non-destructive read
     if old == pos: break                                // slot is ready for us
     // else: consumer hasn't recycled this slot yet, retry

3. WRITE DATA + ADVANCE TURN
   RDMA_WRITE(server.queue[idx].data, val)   // unsignaled, inline, 56B
   RDMA_CAS(server.queue[idx].turn, pos, pos+1)  // signaled
   // Both on same QP: RDMA ordering guarantees WRITE completes before CAS.
   // CAS(pos -> pos+1) makes the slot visible to consumers.
```

### Consumer pop()

```
1. CLAIM POSITION
   pos = RDMA_FAA(server.head, +1)          // signaled
   idx = pos & (QUEUE_SIZE - 1)

2. WAIT FOR TURN (spin)
   loop:
     old = RDMA_CAS(server.queue[idx].turn, pos+1, pos+1)  // signaled, non-destructive read
     if old == pos+1: break                                 // producer has written this slot
     // else: producer hasn't published yet, retry

3. READ DATA
   val = RDMA_READ(server.queue[idx].data)   // signaled, 56B

4. ADVANCE TURN (recycle slot)
   RDMA_CAS(server.queue[idx].turn, pos+1, pos + QUEUE_SIZE)  // signaled
   // CAS(pos+1 -> pos+CAPACITY) returns slot to next producer round.
```

## Synra RDMA MPMC (mpmc_synra)

### Producer push(val)

```
1. CLAIM POSITION (primary only)
   pos = RDMA_FAA(server[0].tail, +1)       // signaled, node 0 only
   idx = pos & (QUEUE_SIZE - 1)

2. REPLICATE CLAIM (flat-log CAS to all N replicas)
   for each replica i in 0..N-1:
     RDMA_CAS(server[i].push_log[pos], EMPTY, pos)   // signaled
   // Wait for ALL N completions; each should return EMPTY (success)

3. WAIT FOR TURN (broadcast to all N replicas)
   loop:
     for each replica i in 0..N-1:
       results[i] = RDMA_CAS(server[i].queue[idx].turn, pos, pos)  // signaled
     // Collect ALL N completions before checking
     if ANY results[i] == pos: break       // at least one replica has the expected turn
     // else: all replicas behind, retry entire broadcast

4. WRITE DATA + ADVANCE TURN (broadcast to all N replicas)
   for each replica i in 0..N-1:
     RDMA_WRITE(server[i].queue[idx].data, val)       // unsignaled, inline, 56B
     RDMA_CAS(server[i].queue[idx].turn, pos, pos+1)  // signaled
     // Both on same QP to replica i: write-before-CAS ordering guaranteed
   // Wait for ALL N signaled CAS completions
```

### Consumer pop()

```
1. CLAIM POSITION (primary only)
   pos = RDMA_FAA(server[0].head, +1)       // signaled, node 0 only
   idx = pos & (QUEUE_SIZE - 1)

2. REPLICATE CLAIM (flat-log CAS to all N replicas)
   for each replica i in 0..N-1:
     RDMA_CAS(server[i].pop_log[pos], EMPTY, pos)    // signaled
   // Wait for ALL N completions

3. WAIT FOR TURN (broadcast to all N replicas)
   loop:
     for each replica i in 0..N-1:
       results[i] = RDMA_CAS(server[i].queue[idx].turn, pos+1, pos+1)  // signaled
     if ANY results[i] == pos+1: break

4. READ DATA (from primary only)
   val = RDMA_READ(server[0].queue[idx].data)  // signaled, 56B
   // All replicas have identical data; read from node 0 only.

5. ADVANCE TURN (broadcast to all N replicas)
   for each replica i in 0..N-1:
     RDMA_CAS(server[i].queue[idx].turn, pos+1, pos + QUEUE_SIZE)  // signaled
   // Wait for ALL N completions
```

## MU RDMA MPMC (mpmc_mu)

**Message format (64 bytes, fits inline):**
```
struct Request {
    u8  op;          // Push=1, Pop=2
    u8  reserved;
    u16 client_id;
    u32 req_id;
    u8  payload[56]; // data for Push; ignored for Pop
}

struct Response {
    u8  op;
    u8  status;      // Ok=0, InternalError=1
    u16 client_id;
    u32 req_id;
    u8  payload[56]; // data for Pop; empty for Push
}
```

**Leader state (in addition to queue buffer):**
- `u64 tail, head` — local variables, NOT remote-accessible atomics
- `MutCtx mutations[512]` — pool of in-flight replication contexts
- `deque<PendingOp> pending_pushes, pending_pops` — queued ops waiting for space/data/mutations

### Client push(val) / pop()

```
// Clients are simple RPC callers:
SEND(inline) Request{op=Push, payload=val}   // or op=Pop
// Wait for RECV completion
resp = RECV()
// For Pop: val = resp.payload
```

### Leader: execute_push(client_id, req_id, payload)

```
1. CHECK SPACE
   if tail - head >= QUEUE_SIZE:
     queue to pending_pushes, return       // queue full, will drain later

2. ALLOCATE MUTATION CONTEXT
   mut = alloc_from_pool()
   if none available:
     queue to pending_pushes, return       // will drain when a mutation completes

3. CLAIM POSITION (local, no atomics)
   pos = tail++
   idx = pos & (QUEUE_SIZE - 1)

4. UPDATE LOCAL BUFFER
   memcpy(queue[idx].data, payload, 56)    // write data
   queue[idx].turn = pos + 1               // advance turn (makes slot visible)
   tail_counter = tail                     // update tail in buffer

5. REPLICATE TO FOLLOWERS
   for each follower:
     RDMA_WRITE(follower.queue[idx], local.queue[idx])  // 64B slot (turn+data), unsignaled, inline
     RDMA_WRITE(follower.tail, local.tail)              // 8B tail counter, signaled, inline
     // Both on same QP to this follower: slot write ordered before tail write.
     mut.pending_followers++

6. ON ALL FOLLOWERS ACK (all signaled WRITEs complete):
   SEND(inline) Response{ok, payload=empty} to client
   release mutation to pool
   try_service_pending()
```

### Leader: execute_pop(client_id, req_id)

```
1. CHECK DATA
   if head >= tail:
     queue to pending_pops, return         // queue empty, will drain later

2. ALLOCATE MUTATION CONTEXT
   mut = alloc_from_pool()
   if none available:
     queue to pending_pops, return

3. CLAIM POSITION (local, no atomics)
   pos = head++
   idx = pos & (QUEUE_SIZE - 1)

4. READ DATA LOCALLY
   memcpy(mut.pop_data, queue[idx].data, 56)

5. UPDATE LOCAL BUFFER
   queue[idx].turn = pos + QUEUE_SIZE      // recycle slot for next producer round
   head_counter = head                     // update head in buffer

6. REPLICATE TO FOLLOWERS
   for each follower:
     RDMA_WRITE(follower.queue[idx].turn, local.queue[idx].turn)  // 8B turn, unsignaled, inline
     RDMA_WRITE(follower.head, local.head)                        // 8B head, signaled, inline
     // Same QP: turn write ordered before head write.
     mut.pending_followers++

7. ON ALL FOLLOWERS ACK:
   SEND(inline) Response{ok, payload=pop_data} to client
   release mutation
   try_service_pending()
```