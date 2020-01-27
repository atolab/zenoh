use super::msg::*;

// Note: implement PartialEq trait in a distinct file to allow this code
// to be imported only when required (only in tests so far)

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        return self.has_decorators() == other.has_decorators()
            && self.cid == other.cid
            && self.header == other.header
            && self.body == other.body
            && self.kind == other.kind
            && self.reply_context == other.reply_context
            && self.properties == other.properties
    }
} 
  
  
  