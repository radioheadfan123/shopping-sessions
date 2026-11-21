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

/// User key from sampled user_ids: the most frequent IDs are the first few persons' alias 0
/// (n = i * 512); a candidate must decode almost every distinct sampled ID below `bound` persons.
pub fn user_key(freq: &[&[u8]], distinct: &[&[u8]], bound: u64) -> Option<u64> {
    let ok = |k: u64, ids: &[&[u8]]| ids.iter().filter(|s| id(s, b'u', k).is_some_and(|n| n >> 9 < bound)).count();
    let mut found = None;
    for s in freq {
        let Some(y) = s.strip_prefix(b"u").and_then(b36) else { continue };
        let mut m = y.wrapping_mul(IB) & M40;
        m ^= m >> 17 ^ m >> 34;
        for i in 0..64u64 {
            for a in 0..8u64 {
                let k = (m ^ ((i * 512 + a).wrapping_mul(A))) & M40;
                if found == Some(k) {
                    continue;
                }
                let pre = &distinct[..distinct.len().min(64)];
                if ok(k, pre) * 100 >= pre.len() * 95 && ok(k, distinct) * 100 >= distinct.len() * 97 {
                    if found.is_some() {
                        return None; // two keys fit: trust neither
                    }
                    found = Some(k);
                }
            }
        }
        if found.is_some() {
            return found;
        }
    }
    None
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
}
