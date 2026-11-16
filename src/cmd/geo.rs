//! GEO commands, a port of Redis' geo.c / geohash.c / geohash_helper.c. Points are
//! stored in sorted sets with a 52-bit interleaved geohash as score.

use super::util::*;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::value::Value;
use crate::zset::{ScoreRange, ZSet};
use bytes::Bytes;

const LAT_MIN: f64 = -85.05112878;
const LAT_MAX: f64 = 85.05112878;
const LONG_MIN: f64 = -180.0;
const LONG_MAX: f64 = 180.0;
const STEP_MAX: u8 = 26;
const EARTH_RADIUS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
struct Hash {
    bits: u64,
    step: u8,
}
impl Hash {
    fn is_zero(&self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

#[derive(Clone, Copy, Default)]
struct Area {
    lat_min: f64,
    lat_max: f64,
    lon_min: f64,
    lon_max: f64,
}

fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF];
    const S: [u32; 5] = [1, 2, 4, 8, 16];
    let mut x = xlo as u64;
    let mut y = ylo as u64;
    for i in (0..5).rev() {
        x = (x | (x << S[i])) & B[i];
        y = (y | (y << S[i])) & B[i];
    }
    x | (y << 1)
}

fn deinterleave64(v: u64) -> u64 {
    const B: [u64; 6] = [0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF, 0x00000000FFFFFFFF];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];
    let mut x = v;
    let mut y = v >> 1;
    for i in 0..6 {
        x = (x | (x >> S[i])) & B[i];
        y = (y | (y >> S[i])) & B[i];
    }
    x | (y << 32)
}

fn encode(lon_r: (f64, f64), lat_r: (f64, f64), lon: f64, lat: f64, step: u8) -> Option<Hash> {
    if step > 32 || step == 0 {
        return None;
    }
    if !(LONG_MIN..=LONG_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
        return None;
    }
    if lat < lat_r.0 || lat > lat_r.1 || lon < lon_r.0 || lon > lon_r.1 {
        return None;
    }
    let mut lat_off = (lat - lat_r.0) / (lat_r.1 - lat_r.0);
    let mut lon_off = (lon - lon_r.0) / (lon_r.1 - lon_r.0);
    lat_off *= (1u64 << step) as f64;
    lon_off *= (1u64 << step) as f64;
    Some(Hash { bits: interleave64(lat_off as u32, lon_off as u32), step })
}

fn encode_wgs84(lon: f64, lat: f64, step: u8) -> Option<Hash> {
    encode((LONG_MIN, LONG_MAX), (LAT_MIN, LAT_MAX), lon, lat, step)
}

fn decode(h: Hash) -> Option<Area> {
    if h.is_zero() {
        return None;
    }
    let sep = deinterleave64(h.bits);
    let lat_scale = LAT_MAX - LAT_MIN;
    let lon_scale = LONG_MAX - LONG_MIN;
    let ilat = sep as u32;
    let ilon = (sep >> 32) as u32;
    let div = (1u64 << h.step) as f64;
    Some(Area {
        lat_min: LAT_MIN + (ilat as f64 / div) * lat_scale,
        lat_max: LAT_MIN + ((ilat as f64 + 1.0) / div) * lat_scale,
        lon_min: LONG_MIN + (ilon as f64 / div) * lon_scale,
        lon_max: LONG_MIN + ((ilon as f64 + 1.0) / div) * lon_scale,
    })
}

fn area_center(a: &Area) -> (f64, f64) {
    let x = ((a.lon_min + a.lon_max) / 2.0).clamp(LONG_MIN, LONG_MAX);
    let y = ((a.lat_min + a.lat_max) / 2.0).clamp(LAT_MIN, LAT_MAX);
    (x, y)
}

/// Decode a zset score into (lon, lat).
fn decode_score(score: f64) -> Option<(f64, f64)> {
    decode(Hash { bits: score as u64, step: STEP_MAX }).map(|a| area_center(&a))
}

fn align52(h: Hash) -> u64 {
    h.bits << (52 - h.step as u32 * 2)
}

fn move_x(h: &mut Hash, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = h.bits & 0xaaaaaaaaaaaaaaaa;
    let y = h.bits & 0x5555555555555555;
    let zz = 0x5555555555555555u64 >> (64 - h.step as u32 * 2);
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaaaaaaaaaaaaaau64 >> (64 - h.step as u32 * 2);
    h.bits = x | y;
}

fn move_y(h: &mut Hash, d: i8) {
    if d == 0 {
        return;
    }
    let x = h.bits & 0xaaaaaaaaaaaaaaaa;
    let mut y = h.bits & 0x5555555555555555;
    let zz = 0xaaaaaaaaaaaaaaaau64 >> (64 - h.step as u32 * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555555555555555u64 >> (64 - h.step as u32 * 2);
    h.bits = x | y;
}

/// [north, south, east, west, north_east, north_west, south_east, south_west]
fn neighbors(h: &Hash) -> [Hash; 8] {
    let mk = |dx: i8, dy: i8| {
        let mut n = *h;
        move_x(&mut n, dx);
        move_y(&mut n, dy);
        n
    };
    [mk(0, 1), mk(0, -1), mk(1, 0), mk(-1, 0), mk(1, 1), mk(-1, 1), mk(1, -1), mk(-1, -1)]
}

fn deg_rad(a: f64) -> f64 {
    a * (std::f64::consts::PI / 180.0)
}
fn rad_deg(a: f64) -> f64 {
    a / (std::f64::consts::PI / 180.0)
}

fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

pub fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = deg_rad(lon1);
    let lon2r = deg_rad(lon2);
    let v = ((lon2r - lon1r) / 2.0).sin();
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let lat1r = deg_rad(lat1);
    let lat2r = deg_rad(lat2);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS * a.sqrt().asin()
}

#[derive(Clone, Copy)]
enum ShapeKind {
    Radius(f64),
    Box(f64, f64),
}

#[derive(Clone, Copy)]
struct Shape {
    kind: ShapeKind,
    xy: (f64, f64),
    conversion: f64,
}

fn estimate_steps(mut range: f64, lat: f64) -> u8 {
    if range == 0.0 {
        return 26;
    }
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2;
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

fn bounding_box(s: &Shape) -> [f64; 4] {
    let (lon, lat) = s.xy;
    let (h, w) = match s.kind {
        ShapeKind::Radius(r) => (r * s.conversion, r * s.conversion),
        ShapeKind::Box(w, h) => (s.conversion * h / 2.0, s.conversion * w / 2.0),
    };
    let lat_delta = rad_deg(h / EARTH_RADIUS);
    let top = rad_deg(w / EARTH_RADIUS / deg_rad(lat + lat_delta).cos());
    let bottom = rad_deg(w / EARTH_RADIUS / deg_rad(lat - lat_delta).cos());
    let south = lat < 0.0;
    [if south { lon - bottom } else { lon - top }, lat - lat_delta, if south { lon + bottom } else { lon + top }, lat + lat_delta]
}

/// geohashCalculateAreasByShapeWGS84: the center box plus its 8 neighbours (some zeroed).
fn areas_by_shape(s: &Shape) -> [Hash; 9] {
    let b = bounding_box(s);
    let (min_lon, min_lat, max_lon, max_lat) = (b[0], b[1], b[2], b[3]);
    let (lon, lat) = s.xy;
    let radius = match s.kind {
        ShapeKind::Radius(r) => r,
        ShapeKind::Box(w, h) => ((w / 2.0) * (w / 2.0) + (h / 2.0) * (h / 2.0)).sqrt(),
    } * s.conversion;
    let mut steps = estimate_steps(radius, lat);
    let mut hash = encode_wgs84(lon, lat, steps).unwrap_or_default();
    let mut nb = neighbors(&hash);
    let mut area = decode(hash).unwrap_or_default();
    let dec = |h: Hash| decode(h).unwrap_or_default();
    let (north, south, east, west) = (dec(nb[0]), dec(nb[1]), dec(nb[2]), dec(nb[3]));
    let decrease = north.lat_max < max_lat || south.lat_min > min_lat || east.lon_max < max_lon || west.lon_min > min_lon;
    if steps > 1 && decrease {
        steps -= 1;
        hash = encode_wgs84(lon, lat, steps).unwrap_or_default();
        nb = neighbors(&hash);
        area = decode(hash).unwrap_or_default();
    }
    let z = Hash::default();
    if steps >= 2 {
        if area.lat_min < min_lat {
            nb[1] = z;
            nb[7] = z;
            nb[6] = z;
        }
        if area.lat_max > max_lat {
            nb[0] = z;
            nb[4] = z;
            nb[5] = z;
        }
        if area.lon_min < min_lon {
            nb[3] = z;
            nb[7] = z;
            nb[5] = z;
        }
        if area.lon_max > max_lon {
            nb[2] = z;
            nb[6] = z;
            nb[4] = z;
        }
    }
    [hash, nb[0], nb[1], nb[2], nb[3], nb[4], nb[5], nb[6], nb[7]]
}

struct Point {
    lon: f64,
    lat: f64,
    dist: f64,
    score: f64,
    member: Bytes,
}

fn within(s: &Shape, score: f64) -> Option<(f64, f64, f64)> {
    let (x, y) = decode_score(score)?;
    let d = distance(s.xy.0, s.xy.1, x, y);
    match s.kind {
        ShapeKind::Radius(r) => {
            if d > r * s.conversion {
                return None;
            }
        }
        ShapeKind::Box(w, h) => {
            let (w, h) = (w * s.conversion, h * s.conversion);
            if lat_distance(y, s.xy.1) > h / 2.0 {
                return None;
            }
            if distance(x, y, s.xy.0, y) > w / 2.0 {
                return None;
            }
        }
    }
    Some((x, y, d))
}

fn members_of_all_neighbors(z: &ZSet, s: &Shape, limit: usize) -> Vec<Point> {
    let boxes = areas_by_shape(s);
    let mut out = Vec::new();
    let mut last = 0usize;
    for i in 0..9 {
        let h = boxes[i];
        if h.is_zero() {
            continue;
        }
        if last != 0 && boxes[last] == h {
            continue;
        }
        if !out.is_empty() && limit > 0 && out.len() >= limit {
            break;
        }
        let min = align52(h) as f64;
        let mut hh = h;
        hh.bits += 1;
        let max = align52(hh) as f64;
        let r = ScoreRange { min, max, minex: false, maxex: true };
        for (m, sc) in z.range_by_score(&r, false, 0, None) {
            if let Some((x, y, d)) = within(s, sc) {
                out.push(Point { lon: x, lat: y, dist: d, score: sc, member: m });
            }
            if !out.is_empty() && limit > 0 && out.len() >= limit {
                break;
            }
        }
        last = i;
    }
    out
}

fn fmt_dist(d: f64) -> Frame {
    // fixedpoint_d2string(d, 4)
    let v = (d * 10000.0).round_ties_even() as i64;
    let neg = v < 0;
    let a = v.unsigned_abs();
    let s = format!("{}{}.{:04}", if neg { "-" } else { "" }, a / 10000, a % 10000);
    Frame::bulk(Bytes::from(s))
}

fn coord(v: f64) -> Frame {
    Frame::bulk(Bytes::from(crate::util::fmt_ld17(v)))
}

fn parse_lonlat(a: &[u8], b: &[u8]) -> Result<(f64, f64), Frame> {
    let x = float(a)?;
    let y = float(b)?;
    if !(LONG_MIN..=LONG_MAX).contains(&x) || !(LAT_MIN..=LAT_MAX).contains(&y) {
        return Err(Frame::err(format!("ERR invalid longitude,latitude pair {:.6},{:.6}", x, y)));
    }
    Ok((x, y))
}

fn parse_unit(u: &[u8]) -> Result<f64, Frame> {
    if eq(u, "m") {
        Ok(1.0)
    } else if eq(u, "km") {
        Ok(1000.0)
    } else if eq(u, "ft") {
        Ok(0.3048)
    } else if eq(u, "mi") {
        Ok(1609.34)
    } else {
        Err(Frame::err("ERR unsupported unit provided. please use M, KM, FT, MI"))
    }
}

fn float_msg(a: &[u8], msg: &str) -> Result<f64, Frame> {
    crate::util::parse_f64(a).ok_or_else(|| Frame::err(format!("ERR {}", msg)))
}

fn parse_distance(r: &[u8], u: &[u8]) -> Result<(f64, f64), Frame> {
    let d = float_msg(r, "need numeric radius")?;
    if d < 0.0 {
        return Err(Frame::err("ERR radius cannot be negative"));
    }
    Ok((d, parse_unit(u)?))
}

fn zset_of<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a ZSet>, Frame> {
    match ctx.s.get_type(ctx.db, key, |v| matches!(v, Value::ZSet(_)))? {
        Some(o) => match &o.v {
            Value::ZSet(z) => Ok(Some(z)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

pub fn geoadd(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut longidx = 2;
    let (mut xx, mut nx) = (false, false);
    while longidx < args.len() {
        let o = &args[longidx];
        if eq(o, "nx") {
            nx = true;
        } else if eq(o, "xx") {
            xx = true;
        } else if eq(o, "ch") {
        } else {
            break;
        }
        longidx += 1;
    }
    if (args.len() - longidx) % 3 != 0 || (xx && nx) || args.len() == longidx {
        return err_syntax();
    }
    let mut zargs: Vec<Bytes> = Vec::with_capacity(args.len());
    zargs.push(Bytes::from_static(b"zadd"));
    zargs.extend_from_slice(&args[1..longidx]);
    for t in args[longidx..].chunks(3) {
        let (x, y) = tri!(parse_lonlat(&t[0], &t[1]));
        let h = encode_wgs84(x, y, STEP_MAX).unwrap_or_default();
        let bits = align52(h);
        zargs.push(Bytes::from(bits.to_string()));
        zargs.push(t[2].clone());
    }
    let r = super::zset::zadd(ctx, &zargs);
    if ctx.rewrite.is_none() {
        ctx.propagate_as(zargs);
    }
    r
}

pub fn geohash(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    const ALPHA: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let z = match zset_of(ctx, &args[1]) {
        Err(e) => return e,
        Ok(z) => z,
    };
    let out = args[2..]
        .iter()
        .map(|m| {
            let Some(sc) = z.and_then(|z| z.score(m)) else { return Frame::Null };
            let Some((x, y)) = decode_score(sc) else { return Frame::Null };
            let h = encode((-180.0, 180.0), (-90.0, 90.0), x, y, 26).unwrap_or_default();
            let mut buf = Vec::with_capacity(11);
            for i in 0..11 {
                let idx = if i == 10 { 0 } else { ((h.bits >> (52 - (i + 1) * 5)) & 0x1f) as usize };
                buf.push(ALPHA[idx]);
            }
            Frame::bulk(Bytes::from(buf))
        })
        .collect();
    Frame::Array(out)
}

pub fn geopos(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let z = match zset_of(ctx, &args[1]) {
        Err(e) => return e,
        Ok(z) => z,
    };
    Frame::Array(
        args[2..]
            .iter()
            .map(|m| match z.and_then(|z| z.score(m)).and_then(decode_score) {
                None => Frame::NullArray,
                Some((x, y)) => Frame::Array(vec![coord(x), coord(y)]),
            })
            .collect(),
    )
}

pub fn geodist(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut to_m = 1.0;
    if args.len() == 5 {
        to_m = tri!(parse_unit(&args[4]));
    } else if args.len() > 5 {
        return err_syntax();
    }
    let z = match zset_of(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return Frame::Null,
        Ok(Some(z)) => z,
    };
    let (Some(a), Some(b)) = (z.score(&args[2]), z.score(&args[3])) else { return Frame::Null };
    match (decode_score(a), decode_score(b)) {
        (Some(p), Some(q)) => fmt_dist(distance(p.0, p.1, q.0, q.1) / to_m),
        _ => Frame::Null,
    }
}

const RADIUS_COORDS: u32 = 1;
const RADIUS_MEMBER: u32 = 2;
const RADIUS_NOSTORE: u32 = 4;
const GEOSEARCH: u32 = 8;
const GEOSEARCHSTORE: u32 = 16;

pub fn georadius_keys(args: &[Bytes]) -> Vec<usize> {
    let mut v = vec![1];
    let base = if eq(&args[0], "georadiusbymember") { 5 } else { 6 };
    let mut i = base;
    while i < args.len() {
        if (eq(&args[i], "store") || eq(&args[i], "storedist")) && i + 1 < args.len() {
            v.push(i + 1);
            i += 1;
        } else if eq(&args[i], "count") {
            i += 1;
        }
        i += 1;
    }
    v
}

pub fn georadius(ctx: &mut Ctx, a: &[Bytes]) -> Frame {
    generic(ctx, a, 1, RADIUS_COORDS)
}
pub fn georadiusbymember(ctx: &mut Ctx, a: &[Bytes]) -> Frame {
    generic(ctx, a, 1, RADIUS_MEMBER)
}
pub fn georadius_ro(ctx: &mut Ctx, a: &[Bytes]) -> Frame {
    generic(ctx, a, 1, RADIUS_COORDS | RADIUS_NOSTORE)
}
pub fn georadiusbymember_ro(ctx: &mut Ctx, a: &[Bytes]) -> Frame {
    generic(ctx, a, 1, RADIUS_MEMBER | RADIUS_NOSTORE)
}
pub fn geosearch(ctx: &mut Ctx, a: &[Bytes]) -> Frame {
    generic(ctx, a, 1, GEOSEARCH)
}
pub fn geosearchstore(ctx: &mut Ctx, a: &[Bytes]) -> Frame {
    generic(ctx, a, 2, GEOSEARCH | GEOSEARCHSTORE)
}

fn generic(ctx: &mut Ctx, args: &[Bytes], src: usize, flags: u32) -> Frame {
    let db = ctx.db;
    // Snapshot the zset (needed after mutable store operations).
    let zexists;
    let member_score = |ctx: &mut Ctx, m: &[u8]| -> Result<Option<f64>, Frame> { Ok(zset_of(ctx, &args[src])?.and_then(|z| z.score(m))) };
    match zset_of(ctx, &args[src]) {
        Err(e) => return e,
        Ok(z) => zexists = z.is_some(),
    }
    let mut storekey: Option<Bytes> = None;
    let mut storedist = false;
    let mut shape = Shape { kind: ShapeKind::Radius(0.0), xy: (0.0, 0.0), conversion: 1.0 };
    let base;
    if flags & RADIUS_COORDS != 0 {
        base = 6;
        shape.xy = tri!(parse_lonlat(&args[2], &args[3]));
        let (r, c) = tri!(parse_distance(&args[4], &args[5]));
        shape.kind = ShapeKind::Radius(r);
        shape.conversion = c;
    } else if flags & RADIUS_MEMBER != 0 && !zexists {
        base = 5;
    } else if flags & RADIUS_MEMBER != 0 {
        base = 5;
        match tri!(member_score(ctx, &args[2])).and_then(decode_score) {
            Some(xy) => shape.xy = xy,
            None => return Frame::err("ERR could not decode requested zset member"),
        }
        let (r, c) = tri!(parse_distance(&args[3], &args[4]));
        shape.kind = ShapeKind::Radius(r);
        shape.conversion = c;
    } else {
        base = if flags & GEOSEARCHSTORE != 0 {
            storekey = Some(args[1].clone());
            3
        } else {
            2
        };
    }
    let (mut withdist, mut withhash, mut withcoords) = (false, false, false);
    let (mut frommember, mut fromloc, mut byradius, mut bybox) = (false, false, false, false);
    let mut sort: i32 = 0; // 0 none, 1 asc, 2 desc
    let mut any = false;
    let mut count: i64 = 0;
    let remaining = args.len().saturating_sub(base);
    let mut i = 0;
    while i < remaining {
        let a = &args[base + i];
        if eq(a, "withdist") {
            withdist = true;
        } else if eq(a, "withhash") {
            withhash = true;
        } else if eq(a, "withcoord") {
            withcoords = true;
        } else if eq(a, "any") {
            any = true;
        } else if eq(a, "asc") {
            sort = 1;
        } else if eq(a, "desc") {
            sort = 2;
        } else if eq(a, "count") && i + 1 < remaining {
            count = tri!(int(&args[base + i + 1]));
            if count <= 0 {
                return Frame::err("ERR COUNT must be > 0");
            }
            i += 1;
        } else if eq(a, "store") && i + 1 < remaining && flags & (RADIUS_NOSTORE | GEOSEARCH) == 0 {
            storekey = Some(args[base + i + 1].clone());
            storedist = false;
            i += 1;
        } else if eq(a, "storedist") && i + 1 < remaining && flags & (RADIUS_NOSTORE | GEOSEARCH) == 0 {
            storekey = Some(args[base + i + 1].clone());
            storedist = true;
            i += 1;
        } else if eq(a, "storedist") && flags & GEOSEARCH != 0 && flags & GEOSEARCHSTORE != 0 {
            storedist = true;
        } else if eq(a, "frommember") && i + 1 < remaining && flags & GEOSEARCH != 0 && !fromloc {
            if zexists {
                match tri!(member_score(ctx, &args[base + i + 1])).and_then(decode_score) {
                    Some(xy) => shape.xy = xy,
                    None => return Frame::err("ERR could not decode requested zset member"),
                }
            }
            frommember = true;
            i += 1;
        } else if eq(a, "fromlonlat") && i + 2 < remaining && flags & GEOSEARCH != 0 && !frommember {
            shape.xy = tri!(parse_lonlat(&args[base + i + 1], &args[base + i + 2]));
            fromloc = true;
            i += 2;
        } else if eq(a, "byradius") && i + 2 < remaining && flags & GEOSEARCH != 0 && !bybox {
            let (r, c) = tri!(parse_distance(&args[base + i + 1], &args[base + i + 2]));
            shape.kind = ShapeKind::Radius(r);
            shape.conversion = c;
            byradius = true;
            i += 2;
        } else if eq(a, "bybox") && i + 3 < remaining && flags & GEOSEARCH != 0 && !byradius {
            let w = tri!(float_msg(&args[base + i + 1], "need numeric width"));
            let h = tri!(float_msg(&args[base + i + 2], "need numeric height"));
            if w < 0.0 || h < 0.0 {
                return Frame::err("ERR height or width cannot be negative");
            }
            shape.conversion = tri!(parse_unit(&args[base + i + 3]));
            shape.kind = ShapeKind::Box(w, h);
            bybox = true;
            i += 3;
        } else {
            return err_syntax();
        }
        i += 1;
    }
    if storekey.is_some() && (withdist || withhash || withcoords) {
        return Frame::err(format!(
            "ERR {} is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            if flags & GEOSEARCHSTORE != 0 { "GEOSEARCHSTORE" } else { "STORE option in GEORADIUS" }
        ));
    }
    let cmdname = String::from_utf8_lossy(&args[0]).into_owned();
    if flags & GEOSEARCH != 0 && !(frommember || fromloc) {
        return Frame::err(format!("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {}", cmdname));
    }
    if flags & GEOSEARCH != 0 && !(byradius || bybox) {
        return Frame::err(format!("ERR exactly one of BYRADIUS and BYBOX can be specified for {}", cmdname));
    }
    if any && count == 0 {
        return Frame::err("ERR the ANY argument requires COUNT argument");
    }
    if !zexists {
        if let Some(dst) = storekey {
            if ctx.s.db_delete(db, &dst) {
                ctx.modified(&dst);
                ctx.notify(NOTIFY_GENERIC, "del", &dst);
                ctx.dirty(1);
            }
            return Frame::Integer(0);
        }
        return Frame::Array(vec![]);
    }
    if count != 0 && sort == 0 && !any {
        sort = 1;
    }
    let mut pts = {
        let z = zset_of(ctx, &args[src]).ok().flatten().unwrap();
        members_of_all_neighbors(z, &shape, if any { count as usize } else { 0 })
    };
    if pts.is_empty() && storekey.is_none() {
        return Frame::Array(vec![]);
    }
    let returned = if count == 0 || (pts.len() as i64) < count { pts.len() } else { count as usize };
    if sort == 1 {
        pts.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(std::cmp::Ordering::Equal));
    } else if sort == 2 {
        pts.sort_by(|a, b| b.dist.partial_cmp(&a.dist).unwrap_or(std::cmp::Ordering::Equal));
    }
    pts.truncate(returned);
    match storekey {
        None => {
            let opts = withdist as usize + withcoords as usize + withhash as usize;
            Frame::Array(
                pts.into_iter()
                    .map(|p| {
                        let d = p.dist / shape.conversion;
                        if opts == 0 {
                            return Frame::Bulk(p.member);
                        }
                        let mut v = vec![Frame::Bulk(p.member)];
                        if withdist {
                            v.push(fmt_dist(d));
                        }
                        if withhash {
                            v.push(Frame::Integer(p.score as i64));
                        }
                        if withcoords {
                            v.push(Frame::Array(vec![coord(p.lon), coord(p.lat)]));
                        }
                        Frame::Array(v)
                    })
                    .collect(),
            )
        }
        Some(dst) => {
            let n = pts.len();
            if n > 0 {
                let mut z = ZSet::new();
                for p in pts {
                    let sc = if storedist { p.dist / shape.conversion } else { p.score };
                    z.insert(p.member, sc);
                }
                ctx.s.set_key(db, dst.clone(), Value::ZSet(Box::new(z)), false, true);
                ctx.notify(NOTIFY_ZSET, if flags & GEOSEARCH != 0 { "geosearchstore" } else { "georadiusstore" }, &dst);
                ctx.dirty(n as u64);
            } else if ctx.s.db_delete(db, &dst) {
                ctx.modified(&dst);
                ctx.notify(NOTIFY_GENERIC, "del", &dst);
                ctx.dirty(1);
            }
            Frame::Integer(n as i64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let h = encode_wgs84(13.361389, 38.115556, 26).unwrap();
        let (x, y) = decode_score(align52(h) as f64).unwrap();
        assert!((x - 13.361389).abs() < 1e-5 && (y - 38.115556).abs() < 1e-5);
        // Redis: GEOADD Sicily 13.361389 38.115556 "Palermo" -> score 3479099956230698
        assert_eq!(align52(h), 3479099956230698);
        assert_eq!(crate::util::fmt_ld17(x), "13.36138933897018433");
    }
    #[test]
    fn dist() {
        let a = encode_wgs84(13.361389, 38.115556, 26).unwrap();
        let b = encode_wgs84(15.087269, 37.502669, 26).unwrap();
        let (p, q) = (decode_score(align52(a) as f64).unwrap(), decode_score(align52(b) as f64).unwrap());
        let d = distance(p.0, p.1, q.0, q.1);
        assert!((d - 166274.1516).abs() < 0.01, "{}", d);
    }
}
