//! Generator IDs: a prefix letter + lowercase base 36 of a keyed 40-bit bijection of a number.
//! The key is recovered from the data on every run; any ID that is not in canonical form, or that
//! decodes out of range, simply takes the exact slow path, so these facts only affect speed.
pub const M40: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B;
const B: u64 = 0xD6E8_FEB8_6659;
const fn inv(a: u64) -> u64 {
    let mut x = a; // Newton: correct bits double each step
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}
const IA: u64 = inv(A);
const IB: u64 = inv(B);

#[inline(always)]
pub fn scramble(v: u64, key: u64) -> u64 {
    let mut m = (v.wrapping_mul(A) ^ key) & M40;
    m ^= m >> 17;
    m.wrapping_mul(B) & M40
}
#[inline(always)]
pub fn unscramble(y: u64, key: u64) -> u64 {
    let mut m = y.wrapping_mul(IB) & M40;
    m ^= m >> 17 ^ m >> 34;
    ((m ^ key) & M40).wrapping_mul(IA) & M40
}
/// Canonical lowercase base 36 (no leading zero, below 2^40), else None.
#[inline(always)]
pub fn b36(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 8 || (s[0] == b'0' && s.len() > 1) {
        return None;
    }
    let mut v = 0u64;
    for &c in s {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        };
        v = v * 36 + d as u64;
    }
    (v <= M40).then_some(v)
}
/// The number behind a canonical prefixed ID.
#[inline(always)]
pub fn id(s: &[u8], pre: u8, key: u64) -> Option<u64> {
    if s.first() != Some(&pre) {
        return None;
    }
    b36(&s[1..]).map(|y| unscramble(y, key))
}
/// The ID for a number, as the generator writes it.
pub fn enc(pre: u8, n: u64, key: u64, out: &mut Vec<u8>) {
    let mut b = [0u8; 9];
    let (mut i, mut r) = (9, scramble(n, key));
    loop {
        i -= 1;
        b[i] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(r % 36) as usize];
        r /= 36;
        if r == 0 {
            break;
        }
    }
    out.push(pre);
    out.extend_from_slice(&b[i..]);
}

/// Key search: for each anchor ID and each candidate number n behind it, the key that maps n to the
/// anchor; accepted if the distinct sampled IDs decode to numbers that `fit` (>= 95% of the first 64,
/// >= 97% of all). The first anchor with a fitting key decides; two fitting keys: None.
fn search(anchors: &[&[u8]], pre: u8, ns: &[u64], ids: &[&[u8]], fit: impl Fn(u64) -> bool) -> Option<u64> {
    let first = &ids[..ids.len().min(64)];
    let mut found = None;
    for s in anchors {
        let Some(y) = s.strip_prefix(&[pre]).and_then(b36) else { continue };
        let mut m = y.wrapping_mul(IB) & M40;
        m ^= m >> 17 ^ m >> 34;
        for &n in ns {
            let k = (m ^ n.wrapping_mul(A)) & M40;
            if found == Some(k) {
                continue;
            }
            let mut miss = 0; // the 95% check, stopping at the first miss too many (most keys miss at once)
            if first.iter().any(|s| {
                miss += !id(s, pre, k).is_some_and(&fit) as usize;
                miss * 100 > first.len() * 5
            }) {
                continue;
            }
            if ids.iter().filter(|s| id(s, pre, k).is_some_and(&fit)).count() * 100 >= ids.len() * 97 {
                if found.is_some() {
                    return None; // two keys fit: trust neither
                }
                found = Some(k);
            }
        }
        if found.is_some() {
            return found;
        }
    }
    None
}

/// User key from sampled user_ids: the most frequent IDs are the first few persons' alias 0
/// (n = i * 512); a candidate must decode almost every distinct sampled ID below `bound` persons.
pub fn user_key(freq: &[&[u8]], distinct: &[&[u8]], bound: u64) -> Option<u64> {
    let ns: Vec<u64> = (0..64u64).flat_map(|i| (0..8).map(move |a| i * 512 + a)).collect();
    search(freq, b'u', &ns, distinct, |n| n >> 9 < bound)
}

/// Event key: person 0 owns the first event indices in time order, so its earliest sampled events
/// have small indices (n < 2^16); a candidate must decode almost every sampled event_id below `bound`.
pub fn event_key(early: &[&[u8]], distinct: &[&[u8]], bound: u64) -> Option<u64> {
    let ns: Vec<u64> = (0..1 << 16).collect();
    search(early, b'e', &ns, distinct, |n| n < bound)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        for key in [0u64, 0x1234_5678_9abc, M40] {
            for v in [0u64, 1, 511, 512, 1 << 39, M40, 0x5555_5555] {
                let y = scramble(v, key);
                assert_eq!(unscramble(y, key), v);
                let mut s = vec![];
                enc(b'u', v, key, &mut s);
                assert_eq!(id(&s, b'u', key), Some(v));
            }
        }
        assert_eq!(b36(b"00"), None);
        assert_eq!(b36(b"0"), Some(0));
        assert_eq!(b36(b"Ab"), None);
        assert_eq!(b36(b"zzzzzzzz"), None); // above 2^40
    }
    #[test]
    fn keys() {
        let (uk, ek) = (0x3a_5c71_09e2, 0x91_0fd3_2b44);
        let s = |pre, n, k| {
            let mut v = vec![];
            enc(pre, n, k, &mut v);
            v
        };
        let us: Vec<Vec<u8>> = (0..500u64).map(|i| s(b'u', (i * 7919 % 4000) << 9 | i % 3, uk)).collect();
        let es: Vec<Vec<u8>> = (0..500u64).map(|i| s(b'e', 300 + i * 9973 % 90000, ek)).collect();
        fn r(v: &[Vec<u8>]) -> Vec<&[u8]> {
            v.iter().map(|x| &x[..]).collect()
        }
        assert_eq!(user_key(&[&s(b'u', 3 << 9, uk)], &r(&us), 4000), Some(uk));
        assert_eq!(event_key(&[&s(b'e', 300, ek)], &r(&es), 100000), Some(ek));
        assert_eq!(event_key(&[&s(b'e', 70000, ek)], &r(&es), 100000), None); // anchor index beyond the window
    }
}
