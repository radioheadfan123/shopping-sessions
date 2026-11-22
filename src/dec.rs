//! Generator IDs: a prefix letter + lowercase base 36 of a keyed 40-bit bijection of a number.
//! The key is recovered from the data on every run; any ID that is not in canonical form, or that
//! decodes out of range, simply takes the exact slow path, so these facts only affect speed.
pub const M40: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B;
const B: u64 = 0xD6E8_FEB8_6659;
pub const fn inv(a: u64) -> u64 {
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
    if let Ok(b) = <[u8; 8]>::try_from(s) {
        return b36x8(u64::from_le_bytes(b)); // ~93% of IDs: a 40-bit value has 8 digits
    }
    b36s(s)
}
/// 8 digits at once (byte 0 = leading digit): range checks, digit map and a 3-level multiply tree.
#[inline(always)]
fn b36x8(x: u64) -> Option<u64> {
    const L: u64 = 0x0101_0101_0101_0101;
    const H: u64 = L * 0x80;
    // bytes below 0x80, so adding up to 0x80 cannot carry: the top bit says byte >= lo (or > hi)
    let ge = |lo: u64| x.wrapping_add((0x80 - lo) * L) & H;
    let gt = |hi: u64| x.wrapping_add((0x7f - hi) * L) & H;
    let let_ = ge(0x61) & !gt(0x7a);
    let ok = x & H == 0 && ((ge(0x30) & !gt(0x39)) | let_) == H && x & 0xff != b'0' as u64;
    if !ok {
        return None;
    }
    let d = x - 0x30 * L - (let_ >> 7) * 0x27;
    let d = (d & 0x00ff_00ff_00ff_00ff) * 36 + (d >> 8 & 0x00ff_00ff_00ff_00ff);
    let d = (d & 0x0000_ffff_0000_ffff) * 1296 + (d >> 16 & 0x0000_ffff_0000_ffff);
    let v = (d & 0xffff_ffff) * 1_679_616 + (d >> 32);
    (v <= M40).then_some(v)
}
#[inline(always)]
fn b36s(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 8 || (s[0] == b'0' && s.len() > 1) {
        return None;
    }
    let (mut v, mut bad) = (0u64, 0u8); // branch free: digits are mixed, so branches mispredict
    for &c in s {
        let d = D36[c as usize];
        bad |= d;
        v = v * 36 + d as u64; // at most 8 digits of 255: no overflow
    }
    (bad < 128 && v <= M40).then_some(v)
}
const D36: [u8; 256] = {
    let mut t = [255u8; 256];
    let mut i = 0;
    while i < 36 {
        t[if i < 10 { b'0' + i } else { b'a' + i - 10 } as usize] = i;
        i += 1;
    }
    t
};
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
        assert_eq!(b36(b"a-"), None);
    }
    #[test]
    fn swar_matches_loop() {
        let al = b"0123456789abcdefghijklmnopqrstuvwxyz/:`{AZ\x00\x7f\x80\xff";
        let mut r = 0x9e37_79b9_7f4a_7c15u64;
        for i in 0..2_000_000u64 {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            let mut s = [0u8; 8];
            for (k, c) in s.iter_mut().enumerate() {
                let q = (r >> (k * 7)) as usize;
                // mostly valid digits, sometimes a bad byte
                *c = if q % 16 == 0 { al[36 + (q >> 4) % (al.len() - 36)] } else { al[(q >> 4) % 36] };
            }
            if i % 5 == 0 {
                s[0] = b'0';
            }
            assert_eq!(b36x8(u64::from_le_bytes(s)), b36s(&s), "{:?}", s);
        }
        for s in [b"zzzzzzzz", b"10000000", b"ffffffff", b"gzzzzzzz", b"h0000000", b"0zzzzzzz", b"aaaaaaaa"] {
            assert_eq!(b36(s), b36s(s));
        }
        for b in 0..=255u8 {
            let s = [b, b'1', b'2', b'3', b'4', b'5', b'6', b'7'];
            assert_eq!(b36(&s), b36s(&s));
            let s = [b'1', b'2', b'3', b'4', b'5', b'6', b'7', b];
            assert_eq!(b36(&s), b36s(&s));
        }
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
