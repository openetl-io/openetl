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
	{"names-without-generation", "generation", "runs are named by epoch, operator and key groups, without the generation"},
	{"names-by-group", "restore", "runs are named by operator and key groups only"},
	{"senders-by-connection", "restore", "a keyed lane aligns barriers and takes its watermark across the senders it has connections from, not every lane of the attempt"},
	{"prune-by-epoch", "retention", "retention deletes runs of epochs before the oldest record kept, though a kept record names them for key groups that didn't change"},
	{"ack-read-positions", "exactly-once", "workers acknowledge upstream where they've read to, not the committed record's positions"},
	{"idle-excluded", "watermark", "an idle split is left out of its lane's watermark, and a lane whose splits are all idle out of its receivers', as Flink does; the record keeps each split's own"},
	{"snapshot-unwaited", "restore", "a lane goes on, and its worker sends its share, before its run is written"},
	{"idle-advances-held", "watermark", "an idle split's watermark advances on a timer, also while it waits at a barrier it placed"},
	// Commits and recovery (workers_commit_test.go, variantShapes).
	{"ack-before-commits", "exactly-once", "the coordinator acknowledges an epoch once it has asked every worker to commit, before they answer"},
	{"answer-on-receipt", "exactly-once", "a worker answers a commit request when it takes it, before it commits"},
	{"commit-before-record", "visibility", "a worker commits its transactions once it sent its share, without a request"},
	{"recovery-skips-uncommitted", "exactly-once", "each worker of the next attempt settles the recorded committables of the lanes it runs, so a lane it doesn't have is skipped"},
	{"single-runner-recovery", "exactly-once", "recovery keeps a single runner's rule: it fences each recorded transaction and rolls back to the record before if any marker is missing"},
	{"stale-worker-commits", "generation", "a worker whose producer was fenced starts a new one, and commits its epoch again from what it holds"},
	{"ids-by-generation", "generation", "transactional IDs carry the attempt's generation, so the next attempt's producers never fence an earlier one's"},
	{"resume-prepared", "exactly-once", "with no staging, recovery commits the transaction the record names, kept prepared as Kafka's two-phase commit lets it, and rolls back if it's gone; a stale worker's producers still abort it when they start"},
	{"apply-unfenced", "generation", "a worker's apply doesn't check the destination's fence, so a stale generation's worker applies after a newer one recovered"},
	{"replay-staged-lane", "exactly-once", "recovery redoes from the source a lane that staged, before looking for its marker"},
	{"unstaged-lost-as-applied", "exactly-once", "recovery takes a lane that staged nothing and whose marker is missing as applied"},
	{"staging-unfenced", "generation", "recovery commits a lane's staging under a producer of its own, without fencing the recorded one"},
	{"redo-by-new-layout", "exactly-once", "a redo from the source reads the splits and key groups the new attempt's lane of that number has, not those the committable names"},
	{"replay-own-watermarks", "exactly-once", "a redo from the source closes the windows its own read's watermarks close, not the record's"},
	{"prune-to-head", "restore", "recovery's retention keeps only the record HEAD names, before the epoch is settled"},
	{"replay-not-exact", "exactly-once", "the source's replay of a range returns a row twice"},
	{"routed-own-splits", "exactly-once", "a committable names only its own worker's splits for an exchanged row-wise output, so a redo misses the rows the exchange brought from other workers"},
	{"routed-redo-whole", "exactly-once", "a redo of an exchanged row-wise output keeps the rows of every key group, not only the lane's"},
	{"redo-every-output", "exactly-once", "a redo writes every output's rows of every split the committable names, so a split that reached the lane only through the exchange gives it rows of an output its own lane didn't write there"},
	// State's runs (workers_state_test.go, variantShapes).
	{"state-before-record", "restore", "a lane's state commits when its run is written, in a manifest of its key groups that a restore reads instead of the record"},
	{"restore-by-listing", "restore", "a restore takes every run in the store for its key groups, not the sections the record names"},
	{"restore-whole-sections", "exactly-once", "a restore takes each section the record names that has any of the lane's key groups whole, and names it on whole"},
	{"redo-from-own-record", "exactly-once", "a redo from the source starts from the state of the record it redoes, not the record before"},
	{"prune-unnamed", "restore", "retention deletes every object no record it keeps names, the newest generation's too"},
	{"merge-before-named", "retention", "a merge deletes its inputs once its output is written, before a record names the output"},
	{"merge-drops-tombstones", "restore", "a merge of the newest values sections drops deletions, though older sections it didn't take hold what they delete"},
	{"drop-by-first", "restore", "a set's or timers' section is dropped once the horizon has passed its earliest entry, not its latest"},
	{"drop-tombstones", "restore", "a values section of deletions is dropped though an older section holds what they delete"},
	{"pending-epoch-only", "restore", "the record carries the lane's changes of the epoch, not all of them since its last run"},
	{"pending-to-memory-only", "restore", "a restore reads the record's pending set into memory, and not into its lane's pending set"},
	{"lazy-stale-handles", "restore", "a lazy lane reads key groups from the sections it restored, not those it names now"},
	{"late-before-dedupe", "exactly-once", "a keyed lane sends a row whose window closed to late before it looks for its dedupe ID"},
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
