package sim

import (
	"strings"
	"testing"
)

// brokenVariants each break the workers kind's protocol in one way, and
// name the invariant that must catch it.
var brokenVariants = []struct{ name, invariant, what string }{
	{"silent-exchange", "exactly-once", "frames between engines are lost or repeated, and the connection goes on"},
	{"watermark-max", "aggregate", "a keyed operator's watermark is the greatest of its senders', not the least"},
	{"unaligned", "restore", "a keyed lane takes a sender's frames past its barrier before every sender's has arrived"},
	{"watermark-not-restored", "watermark", "a restore starts each split's watermark over, not from the record"},
	{"engines-any-generation", "generation", "an engine takes an exchange connection of any attempt and generation"},
	{"runners-any-attempt", "generation", "a runner hands a worker's message to the coordinator it runs now, which takes it whatever its attempt and generation"},
	{"names-without-generation", "generation", "snapshot files are named by epoch, operator and key groups, without the generation"},
	{"names-by-group", "restore", "snapshot files are named by operator and key groups only"},
	{"senders-by-connection", "restore", "a keyed lane aligns barriers and takes its watermark across the senders it has connections from, not every lane of the attempt"},
	{"prune-by-epoch", "restore", "retention deletes snapshot files of epochs before the oldest record kept, though a kept record names them for key groups that didn't change"},
	{"ack-read-positions", "exactly-once", "workers acknowledge upstream where they've read to, not the committed record's positions"},
	{"idle-excluded", "watermark", "an idle split is left out of its lane's watermark, and a lane whose splits are all idle out of its receivers', as Flink does; the record keeps each split's own"},
	{"snapshot-unwaited", "restore", "a lane goes on, and its worker sends its share, before its snapshot file is written"},
	{"idle-advances-held", "watermark", "an idle split's watermark advances on a timer, also while it waits at a barrier it placed"},
	// The txn destination's (txnVariants).
	{"ack-before-commits", "exactly-once", "the coordinator acknowledges an epoch once it has asked every worker to commit, before they answer"},
	{"answer-on-receipt", "exactly-once", "a worker answers a commit request when it takes it, before it commits"},
	{"commit-before-record", "visibility", "a worker commits its transactions once it sent its share, without a request"},
	{"recovery-skips-uncommitted", "exactly-once", "each worker of the next attempt settles the recorded committables of the lanes it runs, so a lane it doesn't have is skipped"},
	{"single-runner-recovery", "exactly-once", "recovery keeps a single runner's rule: it fences each recorded transaction and rolls back to the record before if any marker is missing"},
	{"stale-worker-commits", "generation", "a worker whose producer was fenced starts a new one, and commits its epoch again from what it holds"},
	{"ids-by-generation", "generation", "transactional IDs carry the attempt's generation, so the next attempt's producers never fence an earlier one's"},
	{"resume-prepared", "exactly-once", "with no staged copy, recovery commits the transaction the record names, kept prepared as Kafka's two-phase commit lets it, and rolls back if it's gone; a stale worker's producers still abort it when they start"},
}

// TestBroken shows each invariant of the workers kind catches a protocol
// broken the way it guards against. Most are caught within a few seeds;
// a late write over a newer generation's file takes hundreds.
func TestBroken(t *testing.T) {
	for _, v := range brokenVariants {
		t.Run(v.name, func(t *testing.T) {
			seeds := uint64(max(*seedsFlag, 3000))
			for seed := uint64(1); seed <= seeds; seed++ {
				res := runWorkers(seed, v.name)
				for _, p := range res.problems {
					if strings.HasPrefix(p, "["+v.invariant+"]") {
						t.Logf("%s: seed %d, %s: %s", v.what, seed, res.config, p)
						return
					}
				}
			}
			t.Errorf("%s: no seed of %d broke the %s invariant", v.what, seeds, v.invariant)
		})
	}
}
