extern crate async_std;
extern crate byteorder;
extern crate crossbeam;
extern crate rand;
extern crate uuid;

pub mod core;
pub mod io;
pub mod proto;
pub mod session;

use async_std::sync::Arc;


// The ArcSelf trait and the impl_arc_self are a trait and a macro to
// automatically implement for any object the possibility to retrive 
// an Arc to Self from within the object
pub trait ArcSelf {
    fn set_arc_self(&self, arc_self: &Arc<Self>); 
    
    fn get_arc_self(&self) -> Arc<Self>;
}

#[macro_export]
macro_rules! impl_arc_self {
    /***********/
    /* 
        $name MUST declare the following field in its struct
            weak_self: RwLock<Weak<Self>>
    */
    ($name:ident) => {
        impl ArcSelf for $name {
            fn set_arc_self(&self, arc_self: &Arc<Self>) {
                *self.weak_self.try_write().unwrap() = Arc::downgrade(arc_self);
            }
        
            fn get_arc_self(&self) -> Arc<Self> {
                if let Some(a_self) = self.weak_self.try_read().unwrap().upgrade() {
                    return a_self
                }
                panic!("Object not intiliazed!!!");
            }
        }
    };
}