/// Returns a panic payload's text; a payload that is neither `&str` nor `String` becomes a fixed
/// sentence. A `&Box<dyn Any + Send>` passed in place of the payload it holds is itself an `Any`,
/// so the box is looked into rather than called a payload of neither kind.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(inner) = payload.downcast_ref::<Box<dyn std::any::Any + Send>>() {
        return panic_message(inner.as_ref());
    }
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<panic payload is neither &str nor String>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payload_passed_as_its_box_is_read_through_it() {
        let payload = std::panic::catch_unwind(|| panic!("real message")).unwrap_err();
        assert_eq!(panic_message(&payload), "real message");
        assert_eq!(panic_message(&*payload), "real message");
        let neither: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(panic_message(&*neither), "<panic payload is neither &str nor String>");
    }
}
