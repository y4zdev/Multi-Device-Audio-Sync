#[tokio::main]
async fn main() {
    let (tx1, _) = tokio::sync::broadcast::channel::<u8>(10);
    let tx2 = tx1.clone();
    println!("{}", tx1.same_channel(&tx2));
}
