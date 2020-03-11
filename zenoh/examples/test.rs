use std::sync::Arc;


struct S {
    inner: Arc<String>
}

impl S {
    pub fn get(&self) -> Arc<String> {
        self.inner.clone()
    }

    pub fn update(&mut self, addition: &str) {
        Arc::make_mut(&mut self.inner).push_str(addition);
    }
}


fn main() {

    let mut s = S { inner: Arc::new("hello ".to_string()) };

    println!("{}", s.get());

    let read_arc1 = s.get();

    s.update("world");
    println!("{}", s.get());

    println!("{}", read_arc1);

    let read_arc2 = s.get();

    s.update(" !!");


}