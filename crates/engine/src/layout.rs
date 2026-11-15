//! The key groups a job's lanes and writer lanes own, picked together so
//! that lanes feed writer lanes by range. Writer lane w owns the w-th of the
//! writer lanes' contiguous ranges, as an exchange before the writer sends
//! it rows. Each writer lane's range is cut again among the lanes that feed
//! it, a contiguous run of them in lane order: each writer lane gets one
//! lane, and the others go one at a time to the writer lane with the most
//! key groups per lane, the first of those that tie. A lane's range is so
//! always inside its writer lane's, and more lanes feed fewer by merging
//! ranges, never by hashing again. The runner's `plan.NewLayout` is the
//! same.

use std::ops::RangeInclusive;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    lanes: Vec<RangeInclusive<u32>>,
    writers: Vec<RangeInclusive<u32>>,
    writer_of: Vec<usize>,
}

impl Layout {
    /// Lays `lanes` and `writers` out over `groups` key groups: at least one
    /// lane per writer lane, and at least one key group per lane.
    pub fn new(groups: u32, lanes: usize, writers: usize) -> Result<Self, String> {
        if writers < 1 || lanes < writers || (groups as usize) < lanes {
            return Err(format!(
                "{lanes} lanes and {writers} writer lanes over {groups} key groups: \
                 each writer lane needs a lane, and each lane a key group"
            ));
        }
        let (g, n) = (u64::from(groups), writers as u64);
        let ranges: Vec<(u64, u64)> = (0..n)
            .map(|w| (ceil(w * g, n), ceil((w + 1) * g, n) - 1))
            .collect();
        let size = |w: usize| ranges[w].1 - ranges[w].0 + 1;
        let mut feeders = vec![1u64; writers];
        for _ in writers..lanes {
            let mut best: Option<usize> = None;
            for w in 0..writers {
                if feeders[w] < size(w)
                    && best.is_none_or(|b| size(w) * feeders[b] > size(b) * feeders[w])
                {
                    best = Some(w);
                }
            }
            // Some writer lane has fewer lanes than key groups while the
            // lanes are fewer than the key groups.
            if let Some(b) = best {
                feeders[b] += 1;
            }
        }
        let mut out = Layout {
            lanes: Vec::with_capacity(lanes),
            writers: Vec::with_capacity(writers),
            writer_of: Vec::with_capacity(lanes),
        };
        for (w, &(first, last)) in ranges.iter().enumerate() {
            let (s, f) = (size(w), feeders[w]);
            for j in 0..f {
                out.lanes.push(
                    to_u32(first + ceil(j * s, f))..=to_u32(first + ceil((j + 1) * s, f) - 1),
                );
                out.writer_of.push(w);
            }
            out.writers.push(to_u32(first)..=to_u32(last));
        }
        Ok(out)
    }

    /// The writer lane `lane` feeds.
    pub fn writer_of(&self, lane: usize) -> usize {
        self.writer_of[lane]
    }

    /// The key groups lane `lane` owns.
    pub fn lane(&self, lane: usize) -> RangeInclusive<u32> {
        self.lanes[lane].clone()
    }

    /// The lane that owns key group `group`.
    pub fn lane_of(&self, group: u32) -> usize {
        self.lanes
            .partition_point(|r| *r.end() < group)
            .min(self.lanes.len().saturating_sub(1))
    }

    /// The lanes that feed writer lane `writer`, in order.
    pub fn feeders(&self, writer: usize) -> impl Iterator<Item = usize> + '_ {
        (0..self.writer_of.len()).filter(move |&l| self.writer_of[l] == writer)
    }

    #[cfg(test)]
    pub fn writer(&self, writer: usize) -> RangeInclusive<u32> {
        self.writers[writer].clone()
    }
}

fn ceil(a: u64, b: u64) -> u64 {
    a.div_ceil(b)
}

/// A key group, below the u32 count of them.
fn to_u32(g: u64) -> u32 {
    u32::try_from(g).unwrap_or(u32::MAX)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Key groups, lanes and writer lanes, and the lanes' ranges and writer
    /// lanes they give.
    type Case = (u32, usize, usize, &'static [(u32, u32)], &'static [usize]);

    fn ranges(l: &Layout) -> (Vec<(u32, u32)>, Vec<usize>) {
        (
            l.lanes.iter().map(|r| (*r.start(), *r.end())).collect(),
            l.writer_of.clone(),
        )
    }

    #[test]
    fn lanes_cut_their_writer_lanes_ranges() {
        for groups in [1u32, 2, 4, 8, 16, 256] {
            for writers in 1..=9usize {
                for lanes in writers..=(groups as usize).min(40) {
                    let l = Layout::new(groups, lanes, writers).unwrap();
                    let mut next = 0;
                    for (i, r) in l.lanes.iter().enumerate() {
                        assert_eq!(*r.start(), next, "{groups} {lanes} {writers}: lane {i}");
                        assert!(r.start() <= r.end(), "lane {i} owns no key group");
                        let w = l.writer(l.writer_of(i));
                        assert!(w.start() <= r.start() && r.end() <= w.end());
                        next = r.end() + 1;
                    }
                    assert_eq!(next, groups);
                    for w in 0..writers {
                        assert!(l.feeders(w).count() >= 1, "writer lane {w} has no lane");
                        let owned = |g: u32| (g as usize) * writers / (groups as usize);
                        assert!(l.writer(w).all(|g| owned(g) == w), "writer lane {w}");
                    }
                }
            }
        }
        assert!(Layout::new(4, 5, 1).is_err());
        assert!(Layout::new(256, 1, 2).is_err());
        assert!(Layout::new(256, 1, 0).is_err());
    }

    /// The same layouts as the runner's TestLayout.
    #[test]
    fn layouts_match_the_runners() {
        let cases: [Case; 6] = [
            (256, 1, 1, &[(0, 255)], &[0]),
            (
                256,
                4,
                2,
                &[(0, 63), (64, 127), (128, 191), (192, 255)],
                &[0, 0, 1, 1],
            ),
            (256, 3, 2, &[(0, 63), (64, 127), (128, 255)], &[0, 0, 1]),
            (
                256,
                5,
                3,
                &[(0, 42), (43, 85), (86, 128), (129, 170), (171, 255)],
                &[0, 0, 1, 1, 2],
            ),
            (
                8,
                7,
                5,
                &[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 6), (7, 7)],
                &[0, 0, 1, 1, 2, 3, 4],
            ),
            (4, 4, 3, &[(0, 0), (1, 1), (2, 2), (3, 3)], &[0, 0, 1, 2]),
        ];
        for (groups, lanes, writers, want, of) in cases {
            let l = Layout::new(groups, lanes, writers).unwrap();
            assert_eq!(
                ranges(&l),
                (want.to_vec(), of.to_vec()),
                "{groups} {lanes} {writers}"
            );
        }
    }
}
