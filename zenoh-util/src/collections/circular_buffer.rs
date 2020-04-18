use std::collections::VecDeque;


pub(crate) struct CircularBuffer<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    n: usize
}

impl<T> CircularBuffer<T> {
    pub(crate) fn new(capacity: usize) -> CircularBuffer<T> {
        let buffer = VecDeque::<T>::with_capacity(capacity);        
        CircularBuffer {
            buffer, 
            capacity, 
            n: 0
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, elem: T) -> bool {
        if self.n < self.capacity {
            self.buffer.push_back(elem);
            self.n += 1;
            true             
        } else { 
            false 
        }
    }

    #[inline]
    pub(crate) fn pull(&mut self) -> Option<T> {
        let x = self.buffer.pop_front();
        if x.is_some() {
            self.n -= 1;
        }
        x
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn peek(&self) -> Option<&T> {
        self.buffer.get(0)
    }
    
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    pub(crate) fn is_full(&self) -> bool{
        self.n == self.capacity
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.n
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
}
