pub trait Guard {
  fn is_valid(&self) -> bool;
}
impl<T> Guard for Option<T> {
  fn is_valid(&self) -> bool {
    self.is_some()
  }
}
impl<E, T> Guard for Result<E, T> {
  fn is_valid(&self) -> bool {
    self.is_ok()
  }
}

#[macro_export]
macro_rules! guard {
  ($result:expr, $e:tt) => {{
    let res = $result;
    if !res.is_valid() {
      failure::bail!($e)
    } else {
      res.unwrap()
    }
  }};
}
