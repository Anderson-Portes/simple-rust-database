use oxidb::Database;
use std::io::{self, Write};
use anyhow::Result;

fn main() -> Result<()> {
    println!("--- OxiDB REPL (OxiQL v1.0) ---");
    println!("Dica: Use 'SELECT * FROM users', 'INSERT INTO users {{\"name\":\"Alice\"}}' ou 'QUIT' para sair.");
    
    let db = Database::open(".oxidb")?;

    loop {
        print!("oxidb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        if input.to_uppercase() == "QUIT" || input.to_uppercase() == "EXIT" {
            println!("Até logo!");
            break;
        }

        match db.execute(input) {
            Ok(result) => println!("{}", result),
            Err(e) => eprintln!("Erro: {}", e),
        }
        println!();
    }

    Ok(())
}
