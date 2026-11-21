//! Shopping Sessions pipeline: dirty CSVs -> user_daily + campaign_daily Parquet. Rules: dl/rules.md.
//! Plan: parse events in parallel, scattered by event_id hash -> dedup per partition -> rescatter
//! valid events by person -> per person partition: sort, sessionize, attribute clicks, aggregate.
use memchr::memchr;
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::Path;

/// Big blocks (>= 2 MB) come straight from mmap, 2 MB aligned and marked for huge pages, then
/// pre-faulted in one call: fresh 4 KB page faults were about a fifth of the run time.
struct Big;
const BIG: usize = 2 << 20;
unsafe impl std::alloc::GlobalAlloc for Big {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() < BIG || l.align() > BIG {
            return unsafe { std::alloc::System.alloc(l) };
        }
        let n = l.size().next_multiple_of(BIG);
        {
            // freed blocks are kept and handed out again: no munmap, and no kernel page zeroing on reuse
            let mut pool = POOL.lock().unwrap();
            if let Some(i) = (0..pool.len()).filter(|&i| pool[i].1 >= n).min_by_key(|&i| pool[i].1) {
                let (p, m) = pool.swap_remove(i);
                if m > n {
                    pool.push((p + n, m - n));
                }
                return p as *mut u8;
            }
        }
        // carved from one reserved arena: a mmap/munmap/madvise per block takes the mmap write lock,
        // which stalls every other thread's populate and page faults (8 threads ran no faster than 4)
        let base = *ARENA.get_or_init(|| unsafe {
            let p = libc::mmap(std::ptr::null_mut(), ARENA_SZ + BIG, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0);
            if p == libc::MAP_FAILED {
                return 0;
            }
            let a = (p as usize).next_multiple_of(BIG);
            libc::madvise(a as *mut libc::c_void, ARENA_SZ, libc::MADV_HUGEPAGE);
            a
        });
        if base != 0 {
            let off = BUMP.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            if off + n <= ARENA_SZ {
                let a = base + off;
                return a as *mut u8; // no populate: huge-page faults now run in parallel, and untouched headroom is never zeroed
            }
        }
        unsafe {
            let p = libc::mmap(std::ptr::null_mut(), n + BIG, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0);
            if p == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            let a = (p as usize).next_multiple_of(BIG);
            if a > p as usize {
                libc::munmap(p, a - p as usize);
            }
            if a + n < p as usize + n + BIG {
                libc::munmap((a + n) as *mut libc::c_void, p as usize + BIG - a);
            }
            libc::madvise(a as *mut libc::c_void, n, libc::MADV_HUGEPAGE);
            libc::madvise(a as *mut libc::c_void, n, 23); // MADV_POPULATE_WRITE (Linux 5.14+); a no-op error before that
            a as *mut u8
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        if l.size() < BIG || l.align() > BIG {
            unsafe { std::alloc::System.dealloc(p, l) }
        } else {
            POOL.lock().unwrap().push((p as usize, l.size().next_multiple_of(BIG)));
        }
    }
}
/// Freed big blocks (address, bytes). ponytail: no coalescing of neighbours; add it if a big
/// request ever misses a pool that holds enough adjacent bytes.
static POOL: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Vec::new());
/// Reserved address space only (NORESERVE); pages are committed as blocks are handed out.
const ARENA_SZ: usize = 64 << 30;
static ARENA: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static BUMP: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[global_allocator]
static GA: Big = Big;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const PB: u32 = 10; // partition bits
const P: usize = 1 << PB; // partitions, picked by the top PB bits of a hash
const BAD: u32 = u32::MAX;
fn kn(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
const _: () = assert!(std::mem::size_of::<Ev>() == 32);

#[derive(Default)]
struct IdH(u64);
impl Hasher for IdH {
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x
    }
    fn finish(&self) -> u64 {
        self.0
    }
}
type Map<V> = HashMap<u64, V, BuildHasherDefault<IdH>>; // keys are already hashes

/// Flat open-addressing u64 -> u64 table, one cache line per probe, so lookups can be prefetched.
/// Key 0 marks an empty slot; a real 0 key is stored as 1 (a 2^-64 merge, below the hash's own collision odds).
struct Tab {
    s: Vec<(u64, u64)>,
    mask: usize,
}
impl Tab {
    fn new(n: usize) -> Tab {
        let cap = (n * 2).next_power_of_two().max(16);
        Tab { s: vec![(0, 0); cap], mask: cap - 1 }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        k.max(1) as usize & self.mask
    }
    fn insert(&mut self, k: u64, v: u64) {
        let (k, mut i) = (k.max(1), self.slot(k));
        while self.s[i].0 != 0 && self.s[i].0 != k {
            i = (i + 1) & self.mask;
        }
        self.s[i] = (k, v);
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let (k, mut i) = (k.max(1), self.slot(k));
        loop {
            let (a, v) = self.s[i];
            if a == k {
                return Some(v);
            }
            if a == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        use std::arch::x86_64::*;
        unsafe { _mm_prefetch(self.s.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0) }
    }
}
const PF: usize = 16; // prefetch distance in rows

thread_local! {
    static WC: std::cell::RefCell<Option<Wc<Ev>>> = const { std::cell::RefCell::new(None) };
    /// Event dedup scratch (hash table, winning rows), kept per thread across partitions.
    static SCRATCH: std::cell::RefCell<(Vec<u32>, Vec<D>)> = Default::default();
    static WC2: std::cell::RefCell<Option<(Wc<R>, Wc<D>)>> = const { std::cell::RefCell::new(None) };
}

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = a as u128 * b as u128;
    r as u64 ^ (r >> 64) as u64
}
#[inline(always)]
fn r4(s: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(s[i..i + 4].try_into().unwrap()) as u64
}
#[inline(always)]
fn r8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

/// 64-bit string hash (wyhash's short-input path). ponytail: 64 bits, so ~1e-4 odds of one event_id
/// collision at 60M events; 128 bits if that ever bites.
#[inline]
fn hash(s: &[u8]) -> u64 {
    const S0: u64 = 0xa076_1d64_78bd_642f;
    const S1: u64 = 0xe703_7ed1_a0b4_28db;
    let n = s.len();
    let mut seed = S0;
    let (a, b) = if n <= 16 {
        if n >= 4 {
            let q = (n >> 3) << 2;
            (r4(s, 0) << 32 | r4(s, q), r4(s, n - 4) << 32 | r4(s, n - 4 - q))
        } else if n > 0 {
            ((s[0] as u64) << 16 | (s[n >> 1] as u64) << 8 | s[n - 1] as u64, 0)
        } else {
            (0, 0)
        }
    } else {
        let mut i = 0;
        while n - i > 16 {
            seed = mum(r8(s, i) ^ S1, r8(s, i + 8) ^ seed);
            i += 16;
        }
        (r8(s, n - 16), r8(s, n - 8))
    };
    mum(S1 ^ n as u64, mum(a ^ S1, b ^ seed))
}

/// A string inside a leaked mmap, packed as pointer | len << 48.
#[inline]
fn loc(s: &'static [u8]) -> u64 {
    s.as_ptr() as u64 | (s.len().min(0xffff) as u64) << 48
}
#[inline]
fn at(l: u64) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts((l & 0xffff_ffff_ffff) as *const u8, (l >> 48) as usize) }
}

fn map(path: &Path) -> &'static [u8] {
    use std::os::fd::AsRawFd;
    let f = std::fs::File::open(path).unwrap();
    let n = f.metadata().unwrap().len() as usize;
    if n == 0 {
        return &[];
    }
    unsafe {
        let pop = if std::env::var("POP").is_ok() { libc::MAP_POPULATE } else { 0 };
        let p = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, libc::MAP_PRIVATE | pop, f.as_raw_fd(), 0);
        assert!(p != libc::MAP_FAILED, "mmap {path:?}");
        libc::madvise(p, n, libc::MADV_WILLNEED); // async readahead: a cold page cache fills while earlier files parse
        std::slice::from_raw_parts(p as *const u8, n)
    }
}

fn csvs(dir: &Path) -> Vec<&'static [u8]> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .map(|r| r.filter_map(|e| e.ok()).map(|e| e.path()).collect())
        .unwrap_or_default();
    v.retain(|p: &std::path::PathBuf| p.extension().is_some_and(|x| x == "csv"));
    v.par_iter().map(|p| map(p)).collect()
}

/// Pieces of about CHUNK bytes ending on a line break; the bool says "starts the file" (skip the header).
fn chunks(files: &[&'static [u8]]) -> Vec<(&'static [u8], bool)> {
    let mut out = vec![];
    let chunk = kn("CH", 15) << 20;
    for &d in files {
        let mut s = 0;
        while s < d.len() {
            let e = if d.len() - s <= chunk { d.len() } else { memchr(b'\n', &d[s + chunk..]).map_or(d.len(), |p| s + chunk + p + 1) };
            out.push((&d[s..e], s == 0));
            s = e;
        }
    }
    out
}

#[inline]
fn lines(d: &'static [u8], header: bool, mut f: impl FnMut(&'static [u8])) {
    let mut s = 0;
    let mut first = header;
    while s < d.len() {
        let e = memchr(b'\n', &d[s..]).map_or(d.len(), |p| s + p);
        if !first {
            f(&d[s..e]);
        }
        first = false;
        s = e + 1;
    }
}

/// Split one CSV line into exactly out.len() fields, unquoting; false if malformed.
#[inline]
fn split(line: &'static [u8], out: &mut [&'static [u8]]) -> bool {
    if line.is_empty() {
        return false;
    }
    let (mut k, mut s) = (0, 0);
    loop {
        let (mut e, mut q) = (s, 0);
        while e < line.len() && line[e] != b',' {
            q += (line[e] == b'"') as u32;
            e += 1;
        }
        if k == out.len() {
            return false;
        }
        let f = &line[s..e];
        out[k] = if q == 0 {
            f
        } else if q == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
            &f[1..f.len() - 1]
        } else {
            return false;
        };
        k += 1;
        if e == line.len() {
            return k == out.len();
        }
        s = e + 1;
    }
}

/// Bitmasks of ',' , '\n' and '"' over 64 bytes.
#[inline(always)]
fn masks(b: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let (c, n, q) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'"' as i8));
        let (mut cm, mut nm, mut qm) = (0u64, 0u64, 0u64);
        for i in 0..4 {
            let v = _mm_loadu_si128(b.as_ptr().add(16 * i) as *const __m128i);
            cm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, c)) as u32 as u64) << (16 * i);
            nm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, n)) as u32 as u64) << (16 * i);
            qm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u64) << (16 * i);
        }
        (cm, nm, qm)
    }
}

/// Calls f with the unquoted fields of every well-formed line of d (exactly N fields, quotes only
/// around a whole field); skips the first line if header. Same result as lines() + split().
/// Stage 1 lists every ',' / '\n' of a 16 KB window with the quote count before it, branch-free;
/// stage 2 walks that list a whole line at a time.
#[inline(always)]
fn scan_old<const N: usize>(d: &'static [u8], header: bool, mut f: impl FnMut(&[&'static [u8]; N])) {
    const W: usize = 16 << 10;
    assert!(d.len() < u32::MAX as usize - 128);
    let mut e: Vec<(u32, u32)> = Vec::with_capacity(W + 128); // (offset, quotes before it)
    let (mut qtot, mut skip, mut base, mut ls, mut lq) = (0u32, header, 0usize, 0usize, 0u32);
    const NL: u32 = 1 << 31; // set on the quote count of a '\n' entry, so stage 2 never rereads d
    while base < d.len() {
        let stop = (base + W).min(d.len());
        e.reserve(stop - base + 128);
        let (ep, mut n) = (e.as_mut_ptr(), e.len());
        while base < stop {
            let mut tmp = [0u8; 64];
            let blk: &[u8; 64] = if d.len() - base >= 64 {
                d[base..base + 64].try_into().unwrap()
            } else {
                tmp[..d.len() - base].copy_from_slice(&d[base..]);
                tmp[d.len() - base] = b'\n'; // harmless if the data already ends with one
                &tmp
            };
            let (cm, nm, qm) = masks(blk);
            let mut b = cm | nm;
            let cnt = b.count_ones() as usize;
            let mut u = 0;
            loop {
                for _ in 0..8 {
                    let below = b.wrapping_sub(1) & !b;
                    let tz = b.trailing_zeros();
                    unsafe { ep.add(n + u).write(((base as u32).wrapping_add(tz), (qtot + (qm & below).count_ones()) | ((nm >> (tz & 63)) as u32) << 31)) };
                    b &= b.wrapping_sub(1);
                    u += 1;
                }
                if u >= cnt {
                    break;
                }
            }
            n += cnt;
            qtot += qm.count_ones();
            base += 64;
        }
        if base == d.len() && d[d.len() - 1] != b'\n' {
            unsafe { ep.add(n).write((d.len() as u32, qtot | NL)) }; // no break after the last line, and no tail block to add one
            n += 1;
        }
        unsafe { e.set_len(n) };
        let mut k = 0;
        while k < e.len() {
            let Some(j) = e[k..].iter().position(|x| x.1 & NL != 0) else { break }; // line goes on in the next window
            if j == N - 1 && !skip {
                let mut fl: [&'static [u8]; N] = [&[]; N];
                let (mut s, mut q0, mut good) = (ls, lq, true);
                for (j, &(p, q)) in e[k..k + N].iter().enumerate() {
                    let q = q & !NL;
                    let x = unsafe { d.get_unchecked(s..(p as usize).min(d.len())) };
                    let w = q - q0 == 2 && x.len() >= 2 && x[0] == b'"' && x[x.len() - 1] == b'"';
                    good &= q == q0 || w;
                    fl[j] = if w { &x[1..x.len() - 1] } else { x };
                    (s, q0) = (p as usize + 1, q);
                }
                if good {
                    f(&fl);
                }
            }
            skip = false;
            (ls, lq) = (e[k + j].0 as usize + 1, e[k + j].1 & !NL);
            k += j + 1;
        }
        e.drain(..k);
    }
}

/// Same contract as scan_old, one pass, no entry list: fields come straight off the separator mask.
/// A quote anywhere but at a field's first or last byte marks the line "odd", and odd lines go to
/// split() (exact); the rest need only a 2-byte quote check per field.
#[inline(always)]
#[allow(unused_assignments)]
fn scan<const N: usize>(d: &'static [u8], header: bool, mut f: impl FnMut(&[&'static [u8]; N])) {
    let len = d.len();
    let mut fl: [&'static [u8]; N] = [&[]; N];
    let (mut k, mut good, mut odd, mut skip) = (0usize, true, false, header);
    let (mut s, mut ls, mut base, mut carry) = (0usize, 0usize, 0usize, 1u64); // d starts a field
    macro_rules! field {
        ($p:expr) => {{
            let p = $p;
            if k < N {
                let x = unsafe { d.get_unchecked(s..p) };
                let (q0, q1) = (x.first() == Some(&b'"'), x.last() == Some(&b'"'));
                good &= q0 == q1 && (!q0 || x.len() >= 2);
                fl[k] = if q0 && x.len() >= 2 { unsafe { x.get_unchecked(1..x.len() - 1) } } else { x };
            }
            k += 1;
            s = p + 1;
        }};
    }
    macro_rules! eol {
        ($p:expr) => {{
            if !skip {
                if odd {
                    if split(&d[ls..$p], &mut fl) {
                        f(&fl);
                    }
                } else if k == N && good {
                    f(&fl);
                }
            }
            (skip, k, good, odd, ls) = (false, 0, true, false, $p + 1);
        }};
    }
    while base < len {
        let mut tmp = [0u8; 64];
        let blk: &[u8; 64] = if len - base >= 64 {
            d[base..base + 64].try_into().unwrap()
        } else {
            tmp[..len - base].copy_from_slice(&d[base..]);
            tmp[len - base] = b'\n'; // ends a last line with no break; else an empty line, dropped
            &tmp
        };
        let (cm, nm, qm) = masks(blk);
        let sep = cm | nm;
        let next = (base + 64 >= len || matches!(d[base + 64], b',' | b'\n')) as u64;
        let mut bad = qm & !(sep << 1 | carry | sep >> 1 | next << 63); // quotes off a field edge
        carry = sep >> 63;
        let mut b = sep;
        while b != 0 {
            let tz = b.trailing_zeros();
            field!(base + tz as usize);
            if nm >> tz & 1 != 0 {
                let upto = b ^ b.wrapping_sub(1);
                odd |= bad & upto != 0;
                bad &= !upto;
                eol!(base + tz as usize);
            }
            b &= b.wrapping_sub(1);
        }
        odd |= bad != 0;
        base += 64;
    }
    if len % 64 == 0 && len > 0 && d[len - 1] != b'\n' {
        field!(len);
        eol!(len);
    }
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
    (a < 10 && c < 10).then_some((a * 10 + c) as i64)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468
}

/// F1 / F2 / F3 timestamp -> epoch ms; None if bad.
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    if (s.len() == 20 || s.len() == 25) && s[4] == b'-' { iso(s) } else { epoch(s) }
}

#[inline]
fn epoch(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.len() <= 18 {
        // 18 digits cannot overflow i64
        let (mut v, mut bad) = (0i64, false);
        for &c in s {
            let d = c.wrapping_sub(b'0');
            bad |= d > 9;
            v = v.wrapping_mul(10).wrapping_add(d as i64);
        }
        return (!bad).then_some(v);
    }
    let mut v: i64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

#[inline]
fn iso(s: &[u8]) -> Option<i64> {
    // bytes 0..16 XOR the template: digit lanes must come out 0..9 and separator lanes 0, so
    // adding 0x76 (digit) or 0x7f (separator) to the low 7 bits sets a lane's top bit when it is bad
    const H: u64 = 0x8080_8080_8080_8080;
    const DA: u64 = u64::from_le_bytes([0x76, 0x76, 0x76, 0x76, 0x7f, 0x76, 0x76, 0x7f]);
    const DB: u64 = u64::from_le_bytes([0x76, 0x76, 0x7f, 0x76, 0x76, 0x7f, 0x76, 0x76]);
    let a = u64::from_le_bytes(s[0..8].try_into().unwrap()) ^ u64::from_le_bytes(*b"0000-00-");
    let b = u64::from_le_bytes(s[8..16].try_into().unwrap()) ^ u64::from_le_bytes(*b"00T00:00");
    if (((a & !H) + DA) | a | ((b & !H) + DB) | b) & H != 0 || s[16] != b':' {
        return None;
    }
    let g = |x: u64, i: usize| (x >> (8 * i)) as u8 as i64;
    let (y, mo, d) = (g(a, 0) * 1000 + g(a, 1) * 100 + g(a, 2) * 10 + g(a, 3), g(a, 5) * 10 + g(a, 6), g(b, 0) * 10 + g(b, 1));
    let (h, mi, se) = (g(b, 3) * 10 + g(b, 4), g(b, 6) * 10 + g(b, 7), d2(s, 17)?);
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    const MD: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    const CUM: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    if d > MD[mo as usize] + (leap && mo == 2) as i64 {
        return None;
    }
    let days = if (1970..2100).contains(&y) {
        (y - 1970) * 365 + (y - 1969) / 4 + CUM[mo as usize] + (leap && mo > 2) as i64 + d - 1
    } else {
        days_from_civil(y, mo, d)
    };
    let mut t = ((days * 24 + h) * 60 + mi) * 60 + se;
    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let (oh, om) = (d2(s, 20)?, d2(s, 23)?);
        if s[22] != b':' || oh > 23 || om > 59 {
            return None;
        }
        t -= sign * (oh * 60 + om) * 60;
    }
    Some(t * 1000)
}

#[inline]
fn price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    (v >= 1).then_some(v as u32)
}

/// Event as parsed: ts is ms after window_start, BAD if the event fails validation.
#[derive(Clone, Copy, Default)]
struct Ev {
    // 32 B: Wc streams 8 rows as 4 whole cache lines
    eid: u64,
    uh: u64, // hash of user_id, taken while the line is still in cache
    ts: u32,
    ingest: u32, // seconds after window_start - 40 days
    pc: u32,     // price (0 unless a purchase) | category << 24
    off: u32,    // user_id: offset in its chunk << 8 | len (255 = longer, find the end again)
}

/// Back from a chunk-relative user_id to a loc.
#[inline]
fn uloc(d: &'static [u8], off: u32) -> u64 {
    let s = (off >> 8) as usize;
    let mut n = (off & 255) as usize;
    if n == 255 {
        n = d[s..].iter().position(|&c| c == b',' || c == b'"').unwrap(); // the field holds neither
    }
    loc(&d[s..s + n])
}

/// Valid event, keyed by person; ts = KILL once dedup finds a better copy (sorts to the end of its person).
#[derive(Clone, Copy, Default)]
struct R {
    person: u64,
    ts: u32,
    pc: u32,  // as in Ev
    ci: u32,  // chunk, for the user_id
    off: u32, // as in Ev
}
const KILL: u32 = u32::MAX;

/// Dedup key of every event: eid with its top PB bits (implied by the dedup partition) replaced by the
/// person partition of its R row; at = chunk << 20 | flat row index in that chunk's partition, all ones = no R row.
#[derive(Clone, Copy, Default)]
struct D {
    eid: u64,
    ingest: u32,
    at: u32,
}
const LOW: u64 = (1 << (64 - PB)) - 1;

#[derive(Clone, Copy, Default)]
struct Cl {
    person: u64,
    cid: u64, // loc of click_id, for ties
    ts: u32,  // ms after window_start - 7 days
    camp: u32,
}

struct URow {
    name: u64,
    day: i32, // days after the window's first day
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32, // -1 = null
}

struct Ctx {
    ws: i64,
    we: i64,
    prod: Map<u32>,
    unknown: u32,
    camps: Map<u32>,
}

fn window(json: &str, key: &str) -> i64 {
    let r = &json[json.find(key).expect(key) + key.len()..];
    let r = &r[r.find(':').unwrap() + 1..];
    let r = &r[r.find('"').unwrap() + 1..];
    parse_ts(r[..r.find('"').unwrap()].as_bytes()).expect("manifest ts")
}

/// P partitions of about n rows in total, in one block so the allocator can hand it out as huge,
/// pre-faulted pages; a full partition spills into its own Vec.
struct Bk<T> {
    v: Vec<T>,
    n: Vec<u32>,
    cap: usize,
    spill: Vec<Vec<T>>,
}
impl<T: Copy> Bk<T> {
    fn new(n: usize) -> Self {
        let sz = std::mem::size_of::<T>();
        let mut cap = (n / P + n / P / kn("HR", 16) + kn("PAD", 8)).next_multiple_of(8); // 8 rows: whole lines, for streaming
        if kn("FILL", 1) == 1 {
            cap = ((cap * P * sz).next_multiple_of(BIG) / (P * sz)) & !7; // the block is zeroed anyway: use it all
        }
        Bk { v: Vec::with_capacity(cap * P), n: vec![0; P], cap, spill: (0..P).map(|_| vec![]).collect() }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, x: T) {
        let k = self.n[b] as usize;
        if k < self.cap {
            unsafe { self.v.as_mut_ptr().add(b * self.cap + k).write(x) };
            self.n[b] += 1;
        } else {
            self.spill[b].push(x);
        }
    }
    fn get(&self, b: usize) -> [&[T]; 2] {
        [unsafe { std::slice::from_raw_parts(self.v.as_ptr().add(b * self.cap), self.n[b] as usize) }, &self.spill[b]]
    }
    fn len(&self, b: usize) -> usize {
        self.n[b] as usize + self.spill[b].len()
    }
}

/// Write-combining front for a phase 1 Bk<Ev>: 8 rows per partition staged in cache, then
/// flushed with non-temporal stores (no read-for-ownership of the destination lines).
struct Wc<T> {
    buf: Vec<[T; 8]>,
    t: Vec<u32>,
}
impl<T: Copy + Default> Wc<T> {
    const L: usize = 8 * std::mem::size_of::<T>() / 16; // 16 B stores per 8 rows
    fn new() -> Self {
        assert!(8 * std::mem::size_of::<T>() % 64 == 0);
        Wc { buf: vec![[T::default(); 8]; P], t: vec![0; P] }
    }
    #[inline(always)]
    fn push(&mut self, bk: &mut Bk<T>, b: usize, e: T) {
        let t = self.t[b] as usize;
        self.buf[b][t & 7] = e;
        self.t[b] += 1;
        if t & 7 == 7 {
            let k = t - 7;
            if k + 8 <= bk.cap {
                unsafe {
                    use std::arch::x86_64::*;
                    let d = bk.v.as_mut_ptr().add(b * bk.cap + k) as *mut __m128i;
                    let s = self.buf[b].as_ptr() as *const __m128i;
                    if d as usize % 64 == 0 {
                        for i in 0..Self::L {
                            _mm_stream_si128(d.add(i), _mm_loadu_si128(s.add(i)));
                        }
                    } else {
                        std::ptr::copy_nonoverlapping(s, d, Self::L);
                    }
                }
                bk.n[b] = (k + 8) as u32;
            } else {
                bk.spill[b].extend_from_slice(&self.buf[b]);
            }
        }
    }
    fn finish(&self, bk: &mut Bk<T>) {
        unsafe { std::arch::x86_64::_mm_sfence() };
        for b in 0..P {
            let t = self.t[b] as usize;
            let (k, r) = (t & !7, t & 7);
            if bk.n[b] as usize == k && k + r <= bk.cap {
                for e in &self.buf[b][..r] {
                    bk.push(b, *e);
                }
            } else {
                bk.spill[b].extend_from_slice(&self.buf[b][..r]);
            }
        }
    }
}

#[inline(always)]
fn event(f: &[&'static [u8]; 7], cx: &Ctx, base: *const u8) -> Option<Ev> {
    if f[0].is_empty() {
        return None;
    }
    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
    let eid = hash(f[0]);
    let mut e = Ev { eid, off: 0, uh: 0, ts: BAD, ingest: (ingest.saturating_sub(cx.ws - 40 * DAY) / 1000).clamp(0, u32::MAX as i64) as u32, pc: 0 };
    if let Some(ts) = parse_ts(f[2]) {
        if !f[1].is_empty() && ts >= cx.ws && ts < cx.we && !f[5].is_empty() && ingest.saturating_sub(ts) <= DAY {
            let ok = match f[4] {
                b"view" | b"cart" => true,
                b"purchase" => match price(f[6]) {
                    Some(p) => {
                        e.pc = p | *cx.prod.get(&hash(f[5])).unwrap_or(&cx.unknown) << 24; // price <= 1e7 < 2^24
                        true
                    }
                    None => false,
                },
                _ => false,
            };
            if ok {
                e.ts = (ts - cx.ws) as u32;
                e.off = ((f[1].as_ptr() as usize - base as usize) << 8 | f[1].len().min(255)) as u32;
                e.uh = hash(f[1]);
            }
        }
    }
    Some(e)
}

/// Links -> (id hash -> person key, person key -> name loc). Person key = hash of the person's name.
fn persons(files: &[&'static [u8]]) -> (Tab, Map<u64>) {
    person_tab(pairs(files))
}

// (loc, hash) of both sides, hashed in the parallel pass
fn pairs(files: &[&'static [u8]]) -> Vec<[(u64, u64); 2]> {
    chunks(files)
        .par_iter()
        .flat_map_iter(|&(d, h)| {
            let mut v = vec![];
            scan::<2>(d, h, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push([(loc(f[0]), hash(f[0])), (loc(f[1]), hash(f[1]))]);
                }
            });
            v
        })
        .collect()
}

// sequential union-find: runs beside event parsing, so it is off the critical path
fn person_tab(pairs: Vec<[(u64, u64); 2]>) -> (Tab, Map<u64>) {
    let t = std::time::Instant::now();
    let pr = |w: &str| if std::env::var("PROF").is_ok() { eprintln!("  persons {w} {:.1} ms", t.elapsed().as_secs_f64() * 1e3) };
    let mut idx = Tab::new(pairs.len() * 2); // id hash -> index
    let (mut parent, mut ids): (Vec<u32>, Vec<(u64, u64)>) = (vec![], vec![]);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            p[x as usize] = p[p[x as usize] as usize];
            x = p[x as usize];
        }
        x
    }
    for (i, pr) in pairs.iter().enumerate() {
        if let Some(n) = pairs.get(i + PF) {
            idx.prefetch(n[0].1);
            idx.prefetch(n[1].1);
        }
        let mut id = |(l, h): (u64, u64)| match idx.get(h) {
            Some(i) => i as u32,
            None => {
                let i = ids.len() as u32;
                idx.insert(h, i as u64);
                parent.push(i);
                ids.push((l, h));
                i
            }
        };
        let (a, b) = (id(pr[0]), id(pr[1]));
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    pr("union");
    // the rest runs in parallel beside the event parse: read-only finds (union halved the paths),
    // smallest name per set by CAS, then idx's values rewritten in place from index to person key
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    let n = ids.len();
    let root: Vec<u32> = (0..n as u32)
        .into_par_iter()
        .map(|mut x| {
            while parent[x as usize] != x {
                x = parent[x as usize];
            }
            x
        })
        .collect();
    let best: Vec<AtomicU32> = (0..n as u32).into_par_iter().map(AtomicU32::new).collect();
    (0..n).into_par_iter().for_each(|x| {
        let b = &best[root[x] as usize];
        let mut cur = b.load(Relaxed);
        while at(ids[x].0) < at(ids[cur as usize].0) {
            match b.compare_exchange_weak(cur, x as u32, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    });
    pr("best");
    let (sp, mask) = (idx.s.as_mut_ptr() as usize, idx.mask);
    (0..n).into_par_iter().for_each(|x| {
        let key = ids[best[root[x] as usize].load(Relaxed) as usize].1;
        let k = ids[x].1.max(1);
        let s = sp as *mut (u64, u64);
        let mut i = k as usize & mask;
        unsafe {
            while (*s.add(i)).0 != k {
                i = (i + 1) & mask;
            }
            (*s.add(i)).1 = key; // each id owns its slot: disjoint writes
        }
    });
    let names: Map<u64> = (0..n)
        .into_par_iter()
        .filter(|&x| root[x] as usize == x)
        .map(|x| {
            let (name, key) = ids[best[x].load(Relaxed) as usize];
            (key, name)
        })
        .collect();
    pr("maps");
    (idx, names)
}

fn main() {
    let t0 = std::time::Instant::now();
    let lap = |what: &str| if std::env::var("PROF").is_ok() { { let mut r: libc::rusage = unsafe { std::mem::zeroed() }; unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) }; let cpu = |t: libc::timeval| t.tv_sec as f64 * 1e3 + t.tv_usec as f64 / 1e3; eprintln!("{:>7.1} ms {what} cpu {:.0}+{:.0} flt {}", t0.elapsed().as_secs_f64() * 1e3, cpu(r.ru_utime), cpu(r.ru_stime), r.ru_minflt) } };
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or(d.into());
    let data = std::path::PathBuf::from(env("DATA_DIR", "/data"));
    let out = std::path::PathBuf::from(env("OUTPUT_DIR", "/output"));
    let ncpu = env("NCPU", "0").parse().unwrap_or(0);
    // blocks under BIG stay in malloc's heaps when freed (reused without fresh zeroed pages)
    unsafe {
        libc::mallopt(libc::M_MMAP_THRESHOLD, BIG as i32);
        libc::mallopt(libc::M_TRIM_THRESHOLD, i32::MAX);
    }
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let man = std::fs::read_to_string(data.join("manifest.json")).unwrap();
    let (ws, we) = (window(&man, "\"window_start\""), window(&man, "\"window_end\""));
    assert!(we - ws < 40 * DAY);

    // small tables
    let mut catnames: Vec<&'static [u8]> = vec![b"unknown"];
    let mut prodcat: Vec<(u64, &'static [u8])> = vec![];
    lines(map(&data.join("products.csv")), true, |l| {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        if split(l, &mut f) {
            prodcat.push((hash(f[0]), f[1]));
            catnames.push(f[1]);
        }
    });
    catnames.sort_unstable();
    catnames.dedup();
    assert!(catnames.len() <= 256, "category index must fit 8 bits"); // ponytail: generator makes 20
    let catidx = |c: &[u8]| catnames.binary_search(&c).unwrap() as u32;
    let prod: Map<u32> = prodcat.iter().map(|&(h, c)| (h, catidx(c))).collect();
    let (mut campid, mut chan): (Vec<&'static [u8]>, Vec<&'static [u8]>) = (vec![], vec![]);
    lines(map(&data.join("campaigns.csv")), true, |l| {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        if split(l, &mut f) {
            campid.push(f[0]);
            chan.push(f[1]);
        }
    });
    let camps: Map<u32> = campid.iter().enumerate().map(|(i, c)| (hash(c), i as u32)).collect();
    let ncamp = campid.len();
    let cx = Ctx { ws, we, prod, unknown: catidx(b"unknown"), camps };
    lap("small tables");

    if std::env::var("TSCHECK").is_ok() {
        for y in [0i64, 1, 1899, 1900, 1969, 1970, 1972, 2000, 2024, 2025, 2028, 2099, 2100, 2400, 9999] {
            for mo in 1..=12 {
                for d in [1, 28, 29, 30, 31] {
                    let st = format!("{y:04}-{mo:02}-{d:02}T13:45:59Z");
                    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
                    let ok = d <= [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo as usize - 1];
                    let want = ok.then(|| (((days_from_civil(y, mo, d) * 24 + 13) * 60 + 45) * 60 + 59) * 1000);
                    assert_eq!(parse_ts(st.as_bytes()), want, "{st}");
                }
            }
        }
        for (t, w) in [("0001", Some(1)), ("9223372036854775807", Some(i64::MAX)), ("9223372036854775808", None), ("00000000000000000000001", Some(1)), ("12a", None), ("", None), ("2027-12-19T18:13:05+05:30", Some(1829239985000 - 19800000)), ("2027-12-19T18:13:05-24:00", None), ("2027-12-19X18:13:05Z", None), ("2027-1:-19T18:13:05Z", None), ("2027-12-19T18:13-05Z", None), ("2027-12-19T18:1\u{e9}5Z", None), ("2027/12-19T18:13:05Z", None), ("2027-12-19T18:13:5aZ", None)] {
            assert_eq!(parse_ts(t.as_bytes()), w, "{t}");
        }
        eprintln!("ts ok");
        return;
    }
    if std::env::var("LINKS").is_ok() {
        let (pm, nm) = persons(&csvs(&data.join("links")));
        lap(&format!("links only: {} ids, {} names", pm.s.len() / 2, nm.len()));
        return;
    }
    // 1: events by event_id partition, in parallel with links
    if std::env::var("SCANCHECK").is_ok() {
        // scan() must yield exactly what lines() + split() yield, on every input file
        fn check<const N: usize>(files: &[&'static [u8]]) -> usize {
            let mut n = 0;
            for &(d, h) in &chunks(files) {
                let (mut a, mut b): (Vec<[&[u8]; N]>, Vec<[&[u8]; N]>) = (vec![], vec![]);
                scan::<N>(d, h, |f| a.push(*f));
                lines(d, h, |l| {
                    let mut f: [&'static [u8]; N] = [&[]; N];
                    if split(l, &mut f) {
                        b.push(f);
                    }
                });
                assert!(a == b, "scan differs from split");
                n += a.len();
            }
            n
        }
        let e = check::<7>(&csvs(&data.join("events")));
        let c = check::<4>(&csvs(&data.join("clicks")));
        let l = check::<2>(&csvs(&data.join("links")));
        eprintln!("scan ok: {e} events, {c} clicks, {l} links");
        return;
    }
    if let Ok(m) = std::env::var("ONLY") {
        // diag: 1 = scan only, 2 = scan + event(), 3 = full phase 1 without persons
        let ech = chunks(&csvs(&data.join("events")));
        lap("mapped");
        let m: u32 = m.parse().unwrap();
        let tot: u64 = ech
            .par_iter()
            .map(|&(d, h)| {
                let mut x = 0u64;
                if m == 1 {
                    scan::<7>(d, h, |f| x = x.wrapping_add(f[0].len() as u64 ^ f[3].len() as u64));
                } else if m == 2 {
                    scan::<7>(d, h, |f| if let Some(e) = event(f, &cx, d.as_ptr()) { x = x.wrapping_add(e.eid ^ e.uh ^ e.ts as u64 ^ e.pc as u64) });
                } else {
                    let mut b = Bk::new(d.len() / 70);
                    WC.with_borrow_mut(|w| {
                        let w = w.get_or_insert_with(Wc::new);
                        w.t.fill(0);
                        scan::<7>(d, h, |f| {
                            if let Some(e) = event(f, &cx, d.as_ptr()) {
                                w.push(&mut b, (e.eid >> (64 - PB)) as usize, e);
                            }
                        });
                        w.finish(&mut b);
                    });
                    x = b.n.iter().map(|&n| n as u64).sum();
                    std::mem::forget(b);
                }
                x
            })
            .sum();
        lap(&format!("ONLY={m} done {tot}"));
        unsafe { libc::_exit(0) }
    }
    let (lf, ef, cf) = (csvs(&data.join("links")), csvs(&data.join("events")), csvs(&data.join("clicks")));
    let lp = pairs(&lf);
    let ech = chunks(&ef);
    // 1: person table, then events parsed once: an R row by person and a thin D key by eid
    // clicks are parsed while the (mostly serial) person table builds; their person scatter waits for it
    let ((pmap, names), raw) = rayon::join(
        || person_tab(lp),
        || {
            chunks(&cf)
                .par_iter()
                .map(|&(d, h)| {
                    let mut v: Vec<Cl> = Vec::with_capacity(d.len() / 50);
                    scan::<4>(d, h, |f| {
                        if f[0].is_empty() || f[1].is_empty() {
                            return;
                        }
                        let Some(ts) = parse_ts(f[3]) else { return };
                        if ts < ws - WEEK || ts >= we {
                            return;
                        }
                        let Some(&camp) = cx.camps.get(&hash(f[2])) else { return };
                        v.push(Cl { person: hash(f[1]), cid: loc(f[0]), ts: (ts - (ws - WEEK)) as u32, camp });
                    });
                    v
                })
                .collect::<Vec<_>>()
        },
    );
    let person = |u: u64| pmap.get(u).unwrap_or(u);
    lap("links");
    let nolook = std::env::var("NOLOOK").is_ok(); // timing probe only: wrong output
    assert!(ech.len() < 4095, "too many chunks for 12-bit chunk ids");
    let s1: Vec<(Bk<R>, Bk<D>)> = ech
        .par_iter()
        .enumerate()
        .map(|(ci, &(d, h))| {
            assert!(d.len() < 1 << 24, "chunk too long for 24-bit user_id offsets");
            let (mut br, mut bd) = (Bk::new(d.len() / 70), Bk::new(d.len() / 70));
            WC2.with_borrow_mut(|w| {
                let (wr, wd) = w.get_or_insert_with(|| (Wc::new(), Wc::new()));
                wr.t.fill(0);
                wd.t.fill(0);
                let fin = |e: &Ev, wr: &mut Wc<R>, wd: &mut Wc<D>, br: &mut Bk<R>, bd: &mut Bk<D>| {
                    let (mut at, mut pp) = (u32::MAX, 0u64);
                    if e.ts != BAD {
                        let q = if nolook { e.uh } else { person(e.uh) };
                        let b = (q >> (64 - PB)) as usize;
                        let t = wr.t[b];
                        assert!(t < 1 << 20, "partition too big for 20-bit row ids");
                        (at, pp) = ((ci as u32) << 20 | t, b as u64);
                        wr.push(br, b, R { person: q, ts: e.ts, pc: e.pc, ci: ci as u32, off: e.off });
                    }
                    wd.push(bd, (e.eid >> (64 - PB)) as usize, D { eid: e.eid & LOW | pp << (64 - PB), ingest: e.ingest, at });
                };
                // person lookups run PF events behind the parse, so their table lines are prefetched
                let (mut ring, mut k) = ([Ev::default(); PF], 0usize);
                scan::<7>(d, h, |f| {
                    if let Some(e) = event(f, &cx, d.as_ptr()) {
                        if e.ts != BAD {
                            pmap.prefetch(e.uh);
                        }
                        let old = std::mem::replace(&mut ring[k % PF], e);
                        if k >= PF {
                            fin(&old, wr, wd, &mut br, &mut bd);
                        }
                        k += 1;
                    }
                });
                for i in k.saturating_sub(PF)..k {
                    fin(&ring[i % PF], wr, wd, &mut br, &mut bd);
                }
                wr.finish(&mut br);
                wd.finish(&mut bd);
            });
            (br, bd)
        })
        .collect();
    lap("links + events parsed");

    // 2: clicks by person; events deduped on the thin keys, losing copies' R rows marked KILL in place
    let fc = || {
        raw.par_iter()
            .map(|v| {
                let mut b = Bk::new(v.len());
                let mut w = Wc::new();
                for i in 0..v.len() {
                    if let Some(n) = v.get(i + PF) {
                        pmap.prefetch(n.person);
                    }
                    let mut c = v[i];
                    c.person = person(c.person);
                    w.push(&mut b, (c.person >> (64 - PB)) as usize, c);
                }
                w.finish(&mut b);
                b
            })
            .collect::<Vec<_>>()
    };
    let kill = |d: &D| {
        if d.at != u32::MAX {
            let (bk, pp, t) = (&s1[(d.at >> 20) as usize].0, (d.eid >> (64 - PB)) as usize, (d.at & 0xF_FFFF) as usize);
            let [m, sp] = bk.get(pp);
            let r = if t < m.len() { &m[t] } else { &sp[t - m.len()] };
            // each row is killed by the one job that owns its eid: no two threads write the same row
            unsafe { std::ptr::write_volatile(std::ptr::addr_of!(r.ts) as *mut u32, KILL) }
        }
    };
    let fd = || {
        (0..32).into_par_iter().for_each(|k| {
            for p in k * P / 32..(k + 1) * P / 32 {
                SCRATCH.with_borrow_mut(|(tab, best)| {
                    let n: usize = s1.iter().map(|c| c.1.len(p)).sum();
                    let cap = (n * 2).next_power_of_two().max(16);
                    tab.clear();
                    // slot = tag (eid bits under the partition) << rb | row in best; all ones = empty
                    let rb = usize::BITS - n.leading_zeros();
                    assert!(rb <= 24, "partition too big for 32-bit dedup slots");
                    let rmask = (1u32 << rb) - 1;
                    tab.resize(cap, u32::MAX);
                    best.clear();
                    best.reserve(n);
                    let mut ahead = s1.iter().flat_map(|c| c.1.get(p)).flatten().skip(PF);
                    for e in s1.iter().flat_map(|c| c.1.get(p)).flatten() {
                        if let Some(a) = ahead.next() {
                            use std::arch::x86_64::*;
                            unsafe { _mm_prefetch(tab.as_ptr().add(a.eid as usize & (cap - 1)) as *const i8, _MM_HINT_T0) }
                        }
                        let (tag, mut h) = (((e.eid >> (64 - PB - 32)) as u32) & !rmask, e.eid as usize & (cap - 1));
                        loop {
                            let t = tab[h];
                            if t == u32::MAX {
                                tab[h] = tag | best.len() as u32;
                                best.push(*e);
                                break;
                            }
                            if t & !rmask == tag && (best[(t & rmask) as usize].eid ^ e.eid) & LOW == 0 {
                                let b = &mut best[(t & rmask) as usize];
                                // earliest ingest wins, ties to the first seen (chunk order, then row order)
                                if e.ingest < b.ingest {
                                    kill(b);
                                    *b = *e;
                                } else {
                                    kill(e);
                                }
                                break;
                            }
                            h = (h + 1) & (cap - 1);
                        }
                    }
                });
            }
        })
    };
    let (cl, ()) = rayon::join(fc, fd);
    lap("clicks parsed, events deduped");
    let (s2, sd): (Vec<Bk<R>>, Vec<Bk<D>>) = s1.into_iter().unzip();
    drop(sd);
    lap("s1 dropped");

    // 3: per person partition: sessions, credit, assists, daily rows
    let base_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - base_day + 1) as usize;
    let organic = ncamp;
    struct St {
        rows: Vec<URow>,
        camp: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted; index camp * ndays + day
        seen: Vec<u32>,
        stamp: u32,
        ev: Vec<R>, // sort buffers, reused across partitions: no fresh pages to zero each time
        ck: Vec<Cl>,
    }
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let step = |mut st: St, q: usize| -> St {
            {
                let (mut ev, mut ck) = (std::mem::take(&mut st.ev), std::mem::take(&mut st.ck));
                group(&s2, q, |e| (e.person, e.ts), &mut ev);
                group(&cl, q, |c| (c.person, c.ts), &mut ck);
                let (mut i, mut ci) = (0, 0);
                while i < ev.len() {
                    let pk = ev[i].person;
                    let mut j = i;
                    while j < ev.len() && ev[j].person == pk {
                        j += 1;
                    }
                    let next = j;
                    while j > i && ev[j - 1].ts == KILL {
                        j -= 1; // losing copies sort last
                    }
                    while ci < ck.len() && ck[ci].person < pk {
                        ci += 1;
                    }
                    let c0 = ci;
                    while ci < ck.len() && ck[ci].person == pk {
                        ci += 1;
                    }
                    let pc = &ck[c0..ci];
                    if j == i {
                        i = next;
                        continue;
                    }
                    let name = names.get(&pk).copied().unwrap_or_else(|| uloc(ech[ev[i].ci as usize].0, ev[i].off));
                    let (mut lo, mut hi) = (0, 0);
                    let mut row: Option<URow> = None;
                    let mut cats: Vec<(u32, i64)> = vec![];
                    let mut k = i;
                    while k < j {
                        let mut m = k + 1;
                        while m < j && (ev[m].ts - ev[m - 1].ts) as i64 <= GAP {
                            m += 1;
                        }
                        let start = ev[k].ts as i64;
                        let day = ((ws + start).div_euclid(DAY) - base_day) as usize;
                        let (mut rev, mut buy) = (0i64, false);
                        if row.as_ref().is_some_and(|r| r.day as usize != day) {
                            let mut r = row.take().unwrap();
                            r.top = top(&mut cats);
                            st.rows.push(r);
                        }
                        for e in &ev[k..m] {
                            let (price, cat) = ((e.pc & 0xff_ffff) as i64, e.pc >> 24);
                            if price > 0 {
                                rev += price;
                                buy = true;
                                match cats.iter_mut().find(|c| c.0 == cat) {
                                    Some(c) => c.1 += price,
                                    None => cats.push((cat, price)),
                                }
                            }
                        }
                        let r = row.get_or_insert(URow { name, day: day as i32, sessions: 0, events: 0, revenue: 0, top: -1 });
                        r.sessions += 1;
                        r.events += (m - k) as i64;
                        r.revenue += rev;
                        // clicks in [start - 7d, start]; click ts is ms after ws - 7d, so the bounds are start and start + 7d
                        let sc = start + WEEK;
                        while hi < pc.len() && pc[hi].ts as i64 <= sc {
                            hi += 1;
                        }
                        while lo < hi && (pc[lo].ts as i64) < start {
                            lo += 1;
                        }
                        let credit = if lo < hi {
                            let (mut b, mut x) = (hi - 1, hi - 1);
                            while x > lo && pc[x - 1].ts == pc[hi - 1].ts {
                                x -= 1;
                                if at(pc[x].cid) < at(pc[b].cid) {
                                    b = x;
                                }
                            }
                            st.stamp += 1;
                            for c in &pc[lo..hi] {
                                if st.seen[c.camp as usize] != st.stamp {
                                    st.seen[c.camp as usize] = st.stamp;
                                    st.camp[c.camp as usize * ndays + day][3] += 1;
                                }
                            }
                            pc[b].camp as usize
                        } else {
                            organic
                        };
                        let a = &mut st.camp[credit * ndays + day];
                        a[0] += 1;
                        a[1] += buy as i64;
                        a[2] += rev;
                        k = m;
                    }
                    if let Some(mut r) = row {
                        r.top = top(&mut cats);
                        st.rows.push(r);
                    }
                    i = next;
                }
                (st.ev, st.ck) = (ev, ck);
                st
            }
    };
    // 32 jobs of P / 32 partitions; each writes its user_daily part as soon as it is done
    let states: Vec<Vec<[i64; 4]>> = (0..32)
        .into_par_iter()
        .map(|k| {
            let mut st = St { rows: vec![], camp: vec![[0; 4]; (ncamp + 1) * ndays], seen: vec![0; ncamp], stamp: 0, ev: vec![], ck: vec![] };
            for q in k * P / 32..(k + 1) * P / 32 {
                st = step(st, q);
            }
            if k == 0 || !st.rows.is_empty() {
                write_user(&ud.join(format!("part-{k}.parquet")), &st.rows, &catnames, base_day);
            }
            st.camp
        })
        .collect();

    lap("sessions");
    // 4: write
    let mut camp = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for s in &states {
        for (a, b) in camp.iter_mut().zip(s) {
            for t in 0..4 {
                a[t] += b[t];
            }
        }
    }
    {
        {
            let mut rows = vec![];
            for c in 0..=ncamp {
                for d in 0..ndays {
                    let a = camp[c * ndays + d];
                    if a[0] > 0 || a[3] > 0 {
                        let (id, ch): (&[u8], &[u8]) = if c == organic { (b"organic", b"organic") } else { (campid[c], chan[c]) };
                        rows.push((id, ch, (base_day + d as i64) as i32, a));
                    }
                }
            }
            write_camp(&cd.join("part-0.parquet"), &rows);
        }
    }
    lap("written");
    if kn("EXIT", 0) == 1 {
        unsafe { libc::_exit(0) } // files are closed; skip freeing what the kernel frees anyway
    }
}

/// Partition q of every chunk, ordered by key: one counting pass on the person bits under the
/// partition byte (about 8 rows per bucket), then a small sort per bucket.
fn group<T: Copy>(src: &[Bk<T>], q: usize, key: impl Fn(&T) -> (u64, u32), out: &mut Vec<T>) {
    let n: usize = src.iter().map(|c| c.len(q)).sum();
    let bits = (usize::BITS - (n / 8).leading_zeros()).clamp(1, 16);
    let b = |x: &T| (key(x).0 >> (64 - PB - bits)) as usize & ((1 << bits) - 1);
    let mut at = vec![0u32; (1 << bits) + 1];
    for x in src.iter().flat_map(|c| c.get(q)).flatten() {
        at[b(x) + 1] += 1;
    }
    for i in 1..at.len() {
        at[i] += at[i - 1];
    }
    out.clear();
    out.reserve(n);
    let o = out.spare_capacity_mut();
    for x in src.iter().flat_map(|c| c.get(q)).flatten() {
        let k = b(x);
        o[at[k] as usize].write(*x);
        at[k] += 1;
    }
    unsafe { out.set_len(n) };
    let mut s = 0;
    for &e in &at[..1 << bits] {
        out[s..e as usize].sort_unstable_by_key(&key);
        s = e as usize;
    }
}

/// Category with the highest purchase sum, ties to the smallest name (indices are in name order); clears the list.
fn top(cats: &mut Vec<(u32, i64)>) -> i32 {
    let t = cats.iter().min_by_key(|c| (-c.1, c.0)).map_or(-1, |c| c.0 as i32);
    cats.clear();
    t
}

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use std::sync::Arc;

fn writer(path: &Path, schema: &str) -> SerializedFileWriter<std::fs::File> {
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let schema = Arc::new(parquet::schema::parser::parse_message_type(schema).unwrap());
    SerializedFileWriter::new(std::fs::File::create(path).unwrap(), schema, Arc::new(props)).unwrap()
}

macro_rules! col {
    ($rg:expr, $ty:ty, $vals:expr, $def:expr) => {{
        let mut c = $rg.next_column().unwrap().unwrap();
        c.typed::<$ty>().write_batch(&$vals, $def, None).unwrap();
        c.close().unwrap();
    }};
}

fn write_user(path: &Path, rows: &[URow], cats: &[&[u8]], base_day: i64) {
    let mut w = writer(
        path,
        "message user_daily { required binary user_id (STRING); required int32 day (DATE); required int64 sessions; \
         required int64 events; required int64 revenue_cents; optional binary top_category (STRING); }",
    );
    let mut rg = w.next_row_group().unwrap();
    // zero-copy: names point into the input maps, which live until exit
    col!(rg, ByteArrayType, rows.iter().map(|r| ByteArray::from(bytes::Bytes::from_static(at(r.name)))).collect::<Vec<_>>(), None);
    col!(rg, Int32Type, rows.iter().map(|r| (base_day + r.day as i64) as i32).collect::<Vec<_>>(), None);
    col!(rg, Int64Type, rows.iter().map(|r| r.sessions).collect::<Vec<_>>(), None);
    col!(rg, Int64Type, rows.iter().map(|r| r.events).collect::<Vec<_>>(), None);
    col!(rg, Int64Type, rows.iter().map(|r| r.revenue).collect::<Vec<_>>(), None);
    let cb: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(bytes::Bytes::copy_from_slice(c))).collect();
    let tops: Vec<ByteArray> = rows.iter().filter(|r| r.top >= 0).map(|r| cb[r.top as usize].clone()).collect();
    let def: Vec<i16> = rows.iter().map(|r| (r.top >= 0) as i16).collect();
    col!(rg, ByteArrayType, tops, Some(&def[..]));
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_camp(path: &Path, rows: &[(&[u8], &[u8], i32, [i64; 4])]) {
    let mut w = writer(
        path,
        "message campaign_daily { required binary campaign_id (STRING); required binary channel (STRING); \
         required int32 day (DATE); required int64 sessions; required int64 purchasing_sessions; \
         required int64 revenue_cents; required int64 assisted_sessions; }",
    );
    let mut rg = w.next_row_group().unwrap();
    col!(rg, ByteArrayType, rows.iter().map(|r| ByteArray::from(r.0)).collect::<Vec<_>>(), None);
    col!(rg, ByteArrayType, rows.iter().map(|r| ByteArray::from(r.1)).collect::<Vec<_>>(), None);
    col!(rg, Int32Type, rows.iter().map(|r| r.2).collect::<Vec<_>>(), None);
    for t in 0..4 {
        col!(rg, Int64Type, rows.iter().map(|r| r.3[t]).collect::<Vec<_>>(), None);
    }
    rg.close().unwrap();
    w.close().unwrap();
}
