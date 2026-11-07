package dbosbackend

import "context"

// Collect removes finished workflows past their keep.
func (b *Backend) Collect(ctx context.Context) error { return b.collect(ctx) }
