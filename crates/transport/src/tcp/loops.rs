//! Decides which accept loop takes connections from which listening socket, and in what order a
//! loop polls its sockets.

use std::num::NonZeroUsize;

/// The loops that accept from one socket: `home` gets the socket itself, and each of
/// `duplicates` gets a duplicate that shares its accept queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketLoops {
    pub home: usize,
    pub duplicates: Vec<usize>,
}

/// Spreads an address's `sockets` over `loops` accept loops, so that every loop gets at least
/// one socket when there is one.
///
/// With at least as many sockets as loops, no socket is duplicated; with fewer, loop `i` serves
/// a duplicate of socket `i % sockets`.
pub fn socket_loops(sockets: usize, loops: NonZeroUsize) -> Vec<SocketLoops> {
    let loops = loops.get();
    (0..sockets)
        .map(|j| {
            if sockets >= loops {
                SocketLoops {
                    home: j % loops,
                    duplicates: Vec::new(),
                }
            } else {
                SocketLoops {
                    home: j,
                    duplicates: (j + sockets..loops).step_by(sockets).collect(),
                }
            }
        })
        .collect()
}

/// Starts each poll one past the socket that last gave a connection, so a busy socket cannot
/// starve the others.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PollTurn {
    next: usize,
}

impl PollTurn {
    /// Returns each of `sockets` once, starting where the last turn left off.
    #[inline]
    pub fn order(self, sockets: usize) -> impl Iterator<Item = usize> {
        (0..sockets).map(move |k| (self.next + k) % sockets)
    }

    /// Records that socket `index` gave a connection, so the next turn starts past it.
    #[inline]
    pub fn served(&mut self, index: usize, sockets: usize) {
        self.next = (index + 1) % sockets;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loops(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap_or(NonZeroUsize::MIN)
    }

    fn served_by_loop(sockets: usize, n: usize) -> Vec<Vec<usize>> {
        let mut by_loop = vec![Vec::new(); n];
        for (j, place) in socket_loops(sockets, loops(n)).into_iter().enumerate() {
            by_loop[place.home].push(j);
            for d in place.duplicates {
                by_loop[d].push(j);
            }
        }
        by_loop
    }

    #[test]
    fn as_many_sockets_as_loops_is_one_each_and_no_duplicate() {
        let placed = socket_loops(3, loops(3));
        assert!(placed.iter().all(|p| p.duplicates.is_empty()), "{placed:?}");
        assert_eq!(served_by_loop(3, 3), vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn one_socket_more_than_loops_wraps_to_the_first_loop_without_a_duplicate() {
        let placed = socket_loops(4, loops(3));
        assert!(placed.iter().all(|p| p.duplicates.is_empty()), "{placed:?}");
        assert_eq!(served_by_loop(4, 3), vec![vec![0, 3], vec![1], vec![2]]);
    }

    #[test]
    fn one_socket_fewer_than_loops_duplicates_the_first_into_the_last_loop() {
        assert_eq!(
            socket_loops(2, loops(3)),
            vec![
                SocketLoops { home: 0, duplicates: vec![2] },
                SocketLoops { home: 1, duplicates: Vec::new() },
            ]
        );
        assert_eq!(served_by_loop(2, 3), vec![vec![0], vec![1], vec![0]]);
    }

    #[test]
    fn one_socket_is_duplicated_into_every_other_loop() {
        assert_eq!(
            socket_loops(1, loops(4)),
            vec![SocketLoops { home: 0, duplicates: vec![1, 2, 3] }]
        );
    }

    #[test]
    fn every_loop_gets_a_socket_and_every_socket_one_home() {
        for sockets in 1..9 {
            for n in 1..9 {
                let by_loop = served_by_loop(sockets, n);
                assert!(by_loop.iter().all(|s| !s.is_empty()), "{sockets} over {n}: {by_loop:?}");
                assert_eq!(socket_loops(sockets, loops(n)).len(), sockets);
            }
        }
    }

    #[test]
    fn a_turn_starts_past_the_socket_that_last_served() {
        let mut turn = PollTurn::default();
        assert_eq!(turn.order(3).collect::<Vec<_>>(), vec![0, 1, 2]);
        turn.served(1, 3);
        assert_eq!(turn.order(3).collect::<Vec<_>>(), vec![2, 0, 1]);
        turn.served(2, 3);
        assert_eq!(turn.order(3).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(turn.order(0).count(), 0);
    }
}
