fn main() {
    let proto = std::path::Path::new("proto/gwxds.proto");
    println!("cargo:rerun-if-changed={}", proto.display());
    prost_build::compile_protos(&[proto], &[proto.parent().unwrap()]).expect("prost_build");
}
