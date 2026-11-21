//! Shopping Sessions pipeline: dirty CSVs -> user_daily + campaign_daily Parquet. Rules: dl/rules.md.
//! Plan: parse events in parallel, scattered by event_id hash -> dedup per partition -> rescatter
//! valid events by person -> per person partition: sort, sessionize, attribute clicks, aggregate.
use memchr::memchr;
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::Path;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const P: usize = 256; // partitions, picked by the top 8 bits of a hash
const BAD: u32 = u32::MAX;
const CHUNK: usize = 8 << 20;

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

#[inline]
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// 64-bit string hash. ponytail: 64 bits, so ~1e-4 odds of one event_id collision at 60M events; 128 bits if that ever bites.
#[inline]
fn hash(s: &[u8]) -> u64 {
    let mut h = (s.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut c = s.chunks_exact(8);
    for w in &mut c {
        h = mix(h ^ u64::from_le_bytes(w.try_into().unwrap()));
    }
    let r = c.remainder();
    if !r.is_empty() {
        let mut b = [0u8; 8];
        b[..r.len()].copy_from_slice(r);
        h = mix(h ^ u64::from_le_bytes(b));
    }
    mix(h)
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
        let p = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
        assert!(p != libc::MAP_FAILED, "mmap {path:?}");
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
    for &d in files {
        let mut s = 0;
        while s < d.len() {
            let e = if d.len() - s <= CHUNK { d.len() } else { memchr(b'\n', &d[s + CHUNK..]).map_or(d.len(), |p| s + CHUNK + p + 1) };
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

/// Bitmasks of separators (',' or '\n') and of '"' over 64 bytes.
#[inline(always)]
fn masks(b: &[u8; 64]) -> (u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let (c, n, q) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'"' as i8));
        let (mut s, mut m) = (0u64, 0u64);
        for i in 0..4 {
            let v = _mm_loadu_si128(b.as_ptr().add(16 * i) as *const __m128i);
            s |= (_mm_movemask_epi8(_mm_or_si128(_mm_cmpeq_epi8(v, c), _mm_cmpeq_epi8(v, n))) as u32 as u64) << (16 * i);
            m |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u64) << (16 * i);
        }
        (s, m)
    }
}

/// Calls f with the unquoted fields of every well-formed line of d (exactly N fields, quotes only
/// around a whole field); skips the first line if header. Same result as lines() + split().
#[inline(always)]
fn scan<const N: usize>(d: &'static [u8], header: bool, mut f: impl FnMut(&[&'static [u8]; N])) {
    let mut fl: [&'static [u8]; N] = [&[]; N];
    let (mut k, mut bad, mut skip, mut fs, mut qs, mut qtot) = (0usize, false, header, 0usize, 0u32, 0u32);
    let mut sep = |pos: usize, qb: u32, nl: bool, k: &mut usize, bad: &mut bool| {
        let nq = qb - qs;
        qs = qb;
        let x = &d[fs..pos];
        if *k < N {
            fl[*k] = if nq == 0 {
                x
            } else if nq == 2 && x.len() >= 2 && x[0] == b'"' && x[x.len() - 1] == b'"' {
                &x[1..x.len() - 1]
            } else {
                *bad = true;
                x
            };
        }
        *k += 1;
        if nl {
            if skip {
                skip = false;
            } else if !*bad && *k == N {
                f(&fl);
            }
            *k = 0;
            *bad = false;
        }
        fs = pos + 1;
    };
    let mut base = 0;
    while base < d.len() {
        let mut tmp = [0u8; 64];
        let blk: &[u8; 64] = if d.len() - base >= 64 {
            d[base..base + 64].try_into().unwrap()
        } else {
            tmp[..d.len() - base].copy_from_slice(&d[base..]);
            &tmp
        };
        let (mut s, m) = masks(blk);
        while s != 0 {
            let o = s.trailing_zeros();
            s &= s - 1;
            let qb = qtot + (m & ((1u64 << o) - 1)).count_ones();
            sep(base + o as usize, qb, blk[o as usize] == b'\n', &mut k, &mut bad);
        }
        qtot += m.count_ones();
        base += 64;
    }
    if d.last().is_some_and(|&c| c != b'\n') {
        sep(d.len(), qtot, true, &mut k, &mut bad);
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
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let (y, mo, d) = (d2(s, 0)? * 100 + d2(s, 2)?, d2(s, 5)?, d2(s, 8)?);
    let (h, mi, se) = (d2(s, 11)?, d2(s, 14)?, d2(s, 17)?);
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
#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    user: u64,
    loc: u64,
    ts: u32,
    ingest: u32, // seconds after window_start - 40 days
    price: u32,  // 0 unless a purchase
    cat: u32,
}

/// Valid, deduped event, keyed by person.
#[derive(Clone, Copy)]
struct Ev2 {
    person: u64,
    loc: u64,
    ts: u32,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
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

/// P empty partitions with room for about n rows in total.
fn bucket<T>(n: usize) -> Vec<Vec<T>> {
    (0..P).map(|_| Vec::with_capacity(n / P + n / P / 4 + 16)).collect()
}

#[inline(always)]
fn parse_event(f: &[&'static [u8]; 7], cx: &Ctx, out: &mut [Vec<Ev>]) {
    if let Some(e) = event(f, cx) {
        out[(e.eid >> 56) as usize].push(e);
    }
}

#[inline(always)]
fn event(f: &[&'static [u8]; 7], cx: &Ctx) -> Option<Ev> {
    if f[0].is_empty() {
        return None;
    }
    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
    let eid = hash(f[0]);
    let mut e = Ev { eid, user: 0, loc: 0, ts: BAD, ingest: (ingest.saturating_sub(cx.ws - 40 * DAY) / 1000).clamp(0, u32::MAX as i64) as u32, price: 0, cat: 0 };
    if let Some(ts) = parse_ts(f[2]) {
        if !f[1].is_empty() && ts >= cx.ws && ts < cx.we && !f[5].is_empty() && ingest.saturating_sub(ts) <= DAY {
            let ok = match f[4] {
                b"view" | b"cart" => true,
                b"purchase" => match price(f[6]) {
                    Some(p) => {
                        e.price = p;
                        e.cat = *cx.prod.get(&hash(f[5])).unwrap_or(&cx.unknown);
                        true
                    }
                    None => false,
                },
                _ => false,
            };
            if ok {
                e.ts = (ts - cx.ws) as u32;
                e.user = hash(f[1]);
                e.loc = loc(f[1]);
            }
        }
    }
    Some(e)
}

/// Links -> (id hash -> person key, person key -> name loc). Person key = hash of the person's name.
fn persons(files: &[&'static [u8]]) -> (Map<u64>, Map<u64>) {
    let pairs: Vec<(u64, u64)> = chunks(files)
        .par_iter()
        .flat_map_iter(|&(d, h)| {
            let mut v = vec![];
            scan::<2>(d, h, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((loc(f[0]), loc(f[1])));
                }
            });
            v
        })
        .collect();
    let mut idx: Map<u32> = Map::default();
    let (mut parent, mut locs): (Vec<u32>, Vec<u64>) = (vec![], vec![]);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            p[x as usize] = p[p[x as usize] as usize];
            x = p[x as usize];
        }
        x
    }
    for &(a, b) in &pairs {
        let mut id = |l: u64| {
            *idx.entry(hash(at(l))).or_insert_with(|| {
                parent.push(locs.len() as u32);
                locs.push(l);
                locs.len() as u32 - 1
            })
        };
        let (a, b) = (id(a), id(b));
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let n = locs.len();
    let mut best = vec![u32::MAX; n];
    for x in 0..n as u32 {
        let r = find(&mut parent, x) as usize;
        if best[r] == u32::MAX || at(locs[x as usize]) < at(locs[best[r] as usize]) {
            best[r] = x;
        }
    }
    let mut pmap: Map<u64> = Map::default();
    let mut names: Map<u64> = Map::default();
    pmap.reserve(n);
    for x in 0..n {
        let name = locs[best[find(&mut parent, x as u32) as usize] as usize];
        let key = hash(at(name));
        pmap.insert(hash(at(locs[x])), key);
        names.insert(key, name);
    }
    (pmap, names)
}

fn main() {
    let t0 = std::time::Instant::now();
    let lap = |what: &str| if std::env::var("PROF").is_ok() { eprintln!("{:>7.1} ms {what}", t0.elapsed().as_secs_f64() * 1e3) };
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or(d.into());
    let data = std::path::PathBuf::from(env("DATA_DIR", "/data"));
    let out = std::path::PathBuf::from(env("OUTPUT_DIR", "/output"));
    let ncpu = env("NCPU", "0").parse().unwrap_or(0);
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
        for (t, w) in [("0001", Some(1)), ("9223372036854775807", Some(i64::MAX)), ("9223372036854775808", None), ("00000000000000000000001", Some(1)), ("12a", None), ("", None), ("2027-12-19T18:13:05+05:30", Some(1829239985000 - 19800000)), ("2027-12-19T18:13:05-24:00", None)] {
            assert_eq!(parse_ts(t.as_bytes()), w, "{t}");
        }
        eprintln!("ts ok");
        return;
    }
    // 1: events by event_id partition, in parallel with links
    let ((pmap, names), s1) = rayon::join(
        || persons(&csvs(&data.join("links"))),
        || {
            chunks(&csvs(&data.join("events")))
                .par_iter()
                .map(|&(d, h)| {
                    let mut b: Vec<Vec<Ev>> = bucket(d.len() / 70);
                    scan::<7>(d, h, |f| parse_event(f, &cx, &mut b));
                    b
                })
                .collect::<Vec<_>>()
        },
    );
    lap("links + events parsed");
    let person = |u: u64| *pmap.get(&u).unwrap_or(&u);

    // 2: clicks by person; events deduped, then by person
    let (cl, s2) = rayon::join(
        || {
            chunks(&csvs(&data.join("clicks")))
                .par_iter()
                .map(|&(d, h)| {
                    let mut b: Vec<Vec<Cl>> = bucket(d.len() / 50);
                    scan::<4>(d, h, |f| {
                        if f[0].is_empty() || f[1].is_empty() {
                            return;
                        }
                        let Some(ts) = parse_ts(f[3]) else { return };
                        if ts < ws - WEEK || ts >= we {
                            return;
                        }
                        let Some(&camp) = cx.camps.get(&hash(f[2])) else { return };
                        let p = person(hash(f[1]));
                        b[(p >> 56) as usize].push(Cl { person: p, cid: loc(f[0]), ts: (ts - (ws - WEEK)) as u32, camp });
                    });
                    b
                })
                .collect::<Vec<_>>()
        },
        || {
            (0..P)
                .into_par_iter()
                .map(|p| {
                    let n: usize = s1.iter().map(|c| c[p].len()).sum();
                    let cap = (n * 2).next_power_of_two().max(16);
                    let (mut keys, mut slot) = (vec![0u64; cap], vec![u32::MAX; cap]);
                    let mut best: Vec<Ev> = Vec::with_capacity(n);
                    for c in &s1 {
                        for e in &c[p] {
                            let mut h = e.eid as usize & (cap - 1);
                            loop {
                                if slot[h] == u32::MAX {
                                    keys[h] = e.eid;
                                    slot[h] = best.len() as u32;
                                    best.push(*e);
                                    break;
                                }
                                if keys[h] == e.eid {
                                    let b = &mut best[slot[h] as usize];
                                    if e.ingest < b.ingest {
                                        *b = *e;
                                    }
                                    break;
                                }
                                h = (h + 1) & (cap - 1);
                            }
                        }
                    }
                    let mut b: Vec<Vec<Ev2>> = bucket(best.len());
                    for e in best.iter().filter(|e| e.ts != BAD) {
                        let q = person(e.user);
                        b[(q >> 56) as usize].push(Ev2 { person: q, loc: e.loc, ts: e.ts, price: e.price, cat: e.cat });
                    }
                    b
                })
                .collect::<Vec<_>>()
        },
    );
    lap("clicks parsed, events deduped");
    drop(s1);
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
    }
    let states: Vec<St> = (0..P)
        .into_par_iter()
        .fold(
            || St { rows: vec![], camp: vec![[0; 4]; (ncamp + 1) * ndays], seen: vec![0; ncamp], stamp: 0 },
            |mut st, q| {
                let mut ev: Vec<Ev2> = s2.iter().flat_map(|c| c[q].iter().copied()).collect();
                ev.sort_unstable_by_key(|e| (e.person, e.ts));
                let mut ck: Vec<Cl> = cl.iter().flat_map(|c| c[q].iter().copied()).collect();
                ck.sort_unstable_by_key(|c| (c.person, c.ts));
                let (mut i, mut ci) = (0, 0);
                while i < ev.len() {
                    let pk = ev[i].person;
                    let mut j = i;
                    while j < ev.len() && ev[j].person == pk {
                        j += 1;
                    }
                    while ci < ck.len() && ck[ci].person < pk {
                        ci += 1;
                    }
                    let c0 = ci;
                    while ci < ck.len() && ck[ci].person == pk {
                        ci += 1;
                    }
                    let pc = &ck[c0..ci];
                    let name = *names.get(&pk).unwrap_or(&ev[i].loc);
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
                            if e.price > 0 {
                                rev += e.price as i64;
                                buy = true;
                                match cats.iter_mut().find(|c| c.0 == e.cat) {
                                    Some(c) => c.1 += e.price as i64,
                                    None => cats.push((e.cat, e.price as i64)),
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
                    i = j;
                }
                st
            },
        )
        .collect();

    lap("sessions");
    // 4: write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let mut camp = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for s in &states {
        for (a, b) in camp.iter_mut().zip(&s.camp) {
            for t in 0..4 {
                a[t] += b[t];
            }
        }
    }
    rayon::join(
        || {
            states.par_iter().enumerate().filter(|(i, s)| *i == 0 || !s.rows.is_empty()).for_each(|(i, s)| {
                write_user(&ud.join(format!("part-{i}.parquet")), &s.rows, &catnames, base_day);
            })
        },
        || {
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
        },
    );
    lap("written");
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
    col!(rg, ByteArrayType, rows.iter().map(|r| ByteArray::from(at(r.name))).collect::<Vec<_>>(), None);
    col!(rg, Int32Type, rows.iter().map(|r| (base_day + r.day as i64) as i32).collect::<Vec<_>>(), None);
    col!(rg, Int64Type, rows.iter().map(|r| r.sessions).collect::<Vec<_>>(), None);
    col!(rg, Int64Type, rows.iter().map(|r| r.events).collect::<Vec<_>>(), None);
    col!(rg, Int64Type, rows.iter().map(|r| r.revenue).collect::<Vec<_>>(), None);
    let tops: Vec<ByteArray> = rows.iter().filter(|r| r.top >= 0).map(|r| ByteArray::from(cats[r.top as usize])).collect();
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
