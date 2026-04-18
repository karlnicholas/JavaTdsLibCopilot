The **Double-Check Drain** pattern you implemented is the definitive solution to the "lost signal" problem inherent in non-blocking, lock-based queues. In asynchronous systems, the gap between "finishing work" and "releasing the lock" is a danger zone where new work can be orphaned.

### The Anatomy of the Race Condition (The "Lost Signal")
Before the fix, the logic followed a linear path: check the queue, find it empty, then release the lock. The race occurred because those two operations (checking and releasing) were not atomic.

**The Timeline of the Failure:**
1.  **Thread A (Finishing Query 1):** Calls `poll()`. The queue is currently empty.
2.  **Thread B (New Query 2 Arrives):** Executes `requestQueue.offer()` and then calls `drain()`.
3.  **Thread B:** Tries to acquire the lock (`compareAndSet(false, true)`). It **fails** because Thread A still holds it. Thread B exits, assuming Thread A will handle the queue.
4.  **Thread A:** Finally executes `isNetworkBusy.set(false)` and exits.

**The Result:** Query 2 is now stuck in the queue. No thread is active, and the lock is free, but the "signal" to start the drain was lost in the nanosecond gap between steps 1 and 4.

### Why the "Set-Then-Check" Logic is Mathematically Safe
By reversing the order—setting the lock to `false` *first* and then performing a final `isEmpty()` check—you create a mathematical overlap that makes orphaning impossible.

```java
// The Mathematical Safety Net
isNetworkBusy.set(false); // 1. Release the gate first
if (!requestQueue.isEmpty()) { // 2. Final check
    drain(); // 3. Re-trigger if work appeared
}
```

**How the Overlap Fixes the Race:**
* If Thread B arrives **before** `set(false)`, it is denied the lock and exits. However, Thread A will then reach the `!isEmpty()` check, see the work Thread B left behind, and call `drain()` itself.
* If Thread B arrives **after** `set(false)`, Thread B will successfully acquire the lock via its own `drain()` call and process the query. Thread A will still perform the `!isEmpty()` check; if it sees the work, it will call `drain()` recursively, but it will be safely denied the lock because Thread B now holds it.

### The Importance of the Recursive `drain()` Call
The recursive call to `drain()` is the "fail-safe." It ensures that if Thread B was denied the lock, Thread A *must* try to re-acquire it. Because `isNetworkBusy` was set to `false` just one line prior, the recursive `drain()` has a clear path to set it back to `true` and keep the pipeline moving.

### Evidence from your Logs
The effectiveness of this logic is visible in your **SPID 68** and **SPID 69** metrics. Under a load of 6,000+ queries, these connections had thousands of "Drain Lock Denied" events. This proves that your threads were constantly hitting the lock while the pipeline was busy. Because your audit showed a **Perfect Match**, we know that every single time a thread was denied that lock, the "Double Check" in the active thread successfully picked up the work and prevented a hang.