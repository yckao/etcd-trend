// Copyright 2026 etcd-trend contributors.
// SPDX-License-Identifier: Apache-2.0

package backend

// ReplayCommitStats describes a completed local bbolt transaction. It is an
// offline observation hook; it does not force a transaction or hold a reader.
type ReplayCommitStats struct {
	TxID         int
	DBSize       int64
	DBSizeInUse  int64
	FreePages    int
	PendingPages int
	OpenReadTxs  int
}

// ReplayTick reproduces the native backend timer's pending-work check.
func ReplayTick(be Backend) {
	b := be.(*backend)
	if b.batchTx.safePending() != 0 {
		b.batchTx.Commit()
	}
}

func (b *backend) reportReplayCommit(txid int) {
	b.onReplayCommit(b.replayStats(txid))
}

// ReplayStats reads the current backend metrics without opening a transaction
// or forcing a commit. Use only from the quiescent offline replay driver.
// TxID is zero because this sample is not a completed-commit observation.
func ReplayStats(be Backend) ReplayCommitStats {
	return be.(*backend).replayStats(0)
}

func (b *backend) replayStats(txid int) ReplayCommitStats {
	st := b.db.Stats()
	size := b.Size()
	return ReplayCommitStats{
		TxID: txid, DBSize: size,
		DBSizeInUse: size - int64(st.FreePageN)*int64(b.db.Info().PageSize),
		FreePages:   st.FreePageN, PendingPages: st.PendingPageN,
		OpenReadTxs: st.OpenTxN,
	}
}
