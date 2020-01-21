#[inline(always)]
fn cend(s: &str) -> bool {s.is_empty() || s.starts_with('/')}

#[inline(always)]
fn cwild(s: &str) -> bool {s.starts_with('*')}

#[inline(always)]
fn cnext(s: &str) -> &str {&s[1..]}

#[inline(always)]
fn cequal(s1: &str, s2: &str) -> bool {s1.starts_with(&s2[0..1])}

macro_rules! DEFINE_INTERSECT { 
    ($name:ident, $end:ident, $wild:ident, $next:ident, $elem_intersect:ident) => {
        fn $name(c1: &str, c2: &str) -> bool{
            if($end(c1)  && $end(c2))  {return true;}
            if($wild(c1) && $end(c2))  {return $name($next(c1), c2);}
            if($end(c1)  && $wild(c2)) {return $name(c1, $next(c2));}
            if($wild(c1) || $wild(c2)) {
                if($name($next(c1), c2)) {return true;}
                else {return $name(c1, $next(c2));}
            }
            if($end(c1)  || $end(c2))  {return false;}
            if($elem_intersect(c1, c2)) {return $name($next(c1), $next(c2));}
            return false;
        }
    };
}

DEFINE_INTERSECT!(sub_chunk_intersect, cend, cwild, cnext, cequal);

#[inline(always)]
fn chunk_intersect(c1: &str, c2: &str) -> bool{
    if(cend(c1) && !cend(c2)) || (!cend(c1) && cend(c2)) {return false;}
    return sub_chunk_intersect(c1, c2);
}

#[inline(always)]
fn end(s: &str) -> bool {s.is_empty()}

#[inline(always)]
fn wild(s: &str) -> bool {s.starts_with("**/") || s == "**"}

#[inline(always)]
fn next(s: &str) -> &str {
    match s.find('/') {
        Some(idx) => {&s[(idx+1)..]}
        None => ""
    }
}

DEFINE_INTERSECT!(res_intersect, end, wild, next, chunk_intersect);

#[inline(always)]
pub fn intersect(s1: &str, s2: &str) -> bool {
    res_intersect(s1, s2)
}
