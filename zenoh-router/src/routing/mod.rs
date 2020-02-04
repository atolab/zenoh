pub mod tables;
pub mod resource;
pub mod session;

#[macro_export]
macro_rules! opt_match { 
    ($expr:expr ; Some($some:ident) => $sblock:block None => $nblock:block) => {
        {let x = match $expr {
            Some($some) => {Some($sblock)}
            None => None
        };
        match x {
            Some(v) => {v}
            None => $nblock
        }}
    };
    ($expr:expr ; None => $nblock:block Some($some:ident) => $sblock:block) => {
        opt_match!($expr ; Some($some) => $sblock None => $nblock)
    };
}