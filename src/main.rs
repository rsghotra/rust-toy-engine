use std::io;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::io::AsyncBufReadExt;
use tokio::time::{timeout, Duration};
use csv::ReaderBuilder;
use rust_decimal::Decimal;

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TxnKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TxnStatus {
    Okay,
    Disputed
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub kind: TxnKind,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<Decimal>,
}

impl Transaction {
    pub fn new(kind: TxnKind, client: u16, tx: u32, amount: Option<Decimal>) -> Transaction {
        Transaction { kind, client, tx, amount}
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Account {
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}


impl Account {
    pub fn new() -> Account{
        Account {
            available: Decimal::new(0, 4),
            held: Decimal::new(0, 4),
            total: Decimal::new(0, 4),
            locked: false,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        //CLI mode
        let transaction_file = &args[1];
        process_single_file(transaction_file)?;
    } else {
        //TCP server mode - extension for concurrent processing
        info!("Starting TCP server mode..");
        start_tcp_server().await?;
    }
    Ok(())
}

fn process_single_file(file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    //declare global accounts, transaction
    let mut accounts: HashMap<u16, Account> =  HashMap::new();
    let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();

    let mut rdr = ReaderBuilder::new().trim(csv::Trim::All).from_path(file_path)?;
    
    for record in rdr.deserialize::<Transaction>() {
        let transaction = record?;
        process_transaction(&transaction, &mut accounts, &mut transactions);
    }
    
    output_results(&accounts)?;
    Ok(())
}

pub async fn start_tcp_server() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::channel::<Transaction>(100_000);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("TCP server listening on 127.0.0.1:8080...");

    let consumer = tokio::spawn(async move {
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();

        loop {
            match timeout(Duration::from_secs(3), rx.recv()).await {
                Ok(Some(transaction)) => {
                    // Process transaction normally
                    process_transaction(&transaction, &mut accounts, &mut transactions);
                }
                Ok(None) => {
                    // Channel closed - server shutting down
                    info!("=== Channel closed, final results ===");
                    output_results(&accounts).expect("Failed to output results");
                    break;
                }
                Err(_) => {
                    // Timeout - no transactions for 10 seconds
                    info!("=== No activity for 10 seconds, current results ===");
                    output_results(&accounts).expect("Failed to output results");
                    // Continue waiting for more transactions
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl+C → shutting down gracefully…");
                break;
            }
            Ok((socket, addr)) = listener.accept() => {
                info!("New connection from: {}", addr);
                let sender = tx.clone();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(socket);
                    let mut file_path = String::new();
                    if let Ok(_) = reader.read_line(&mut file_path).await {
                        let file_path = file_path.trim().trim_matches('\0').replace('\r', "");
                        info!("Producer {}: Processing CSV file: {}", addr, file_path);
                        match csv::ReaderBuilder::new().trim(csv::Trim::All).from_path(&file_path) {
                            Ok(mut rdr) => {
                                for record in rdr.deserialize::<Transaction>() {
                                    match record {
                                        Ok(t) => { if sender.send(t).await.is_err() { break; } }
                                        Err(e) => eprintln!("Error parsing {}: {}", file_path, e),
                                    }
                                }
                            }
                            Err(e) => eprintln!("Failed to open {}: {}", file_path, e),
                        }
                    }
                });
            }
        }
    }

    drop(tx);              // close channel so consumer gets Ok(None)
    let _ = consumer.await; // wait for final output
    Ok(())
}

fn process_transaction(transaction: &Transaction, accounts: &mut HashMap<u16, Account>, transactions: &mut HashMap<u32, (TxnKind, Decimal, TxnStatus)>) {

    let client = transaction.client;
    if !accounts.contains_key(&client) {
        accounts.insert(client, Account::new());
    }

    if let Some(account) = accounts.get_mut(&client) {
        //skip processing if account is locked
        if account.locked {
            return;
        }

        match transaction.kind {
            TxnKind::Deposit => {
                if let Some(amt) = transaction.amount {
                    account.available += amt;
                    account.total += amt;
                    transactions.insert(transaction.tx, (transaction.kind, amt, TxnStatus::Okay));
                }
            },
            TxnKind::Withdrawal => {
                if let Some(amt) = transaction.amount {
                    if account.available >= amt {
                        account.available -= amt;
                        account.total -= amt;
                        transactions.insert(transaction.tx, (transaction.kind, amt, TxnStatus::Okay));
                    } else {
                        warn!("Withdrawal failed - insufficient funds: client={}, tx={}, amount={}, available={}", 
                                transaction.client, transaction.tx, amt, account.available);
                    }
                }
            },
            TxnKind::Dispute => {
                if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                    if matches!(txn_record.0, TxnKind::Deposit) && txn_record.2 == TxnStatus::Okay {
                        account.held +=  txn_record.1;
                        account.available -= txn_record.1;
                        txn_record.2 = TxnStatus::Disputed;
                    } else {
                        warn!("Dispute failed - transaction not found: client={}, tx={}", 
                            transaction.client, transaction.tx);
                    }
                }
            },
            TxnKind::Resolve => {
                if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                    if txn_record.2 == TxnStatus::Disputed {
                        account.held -=  txn_record.1;
                        account.available += txn_record.1;
                        txn_record.2 = TxnStatus::Okay;
                    } else {
                        warn!("Resolve/Chargeback failed - not disputed: client={}, tx={}", 
                            transaction.client, transaction.tx);
                    }
                } else {
                    warn!("Resolve/Chargeback failed - transaction not found: client={}, tx={}", 
                        transaction.client, transaction.tx);
                }
            },
            TxnKind::Chargeback => {
                if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                    if txn_record.2 == TxnStatus::Disputed {
                        account.held -=  txn_record.1;
                        account.total -= txn_record.1;
                        account.locked = true;
                    }
                }
            },
        }
    }
}

fn output_results(accounts: &HashMap<u16, Account>) -> Result<(), Box<dyn std::error::Error>> {
    //here need to output shit to csv
    let mut output = csv::Writer::from_writer(io::stdout());

    //need to write header manually still
    output.write_record(&["client", "available", "held", "total", "locked"])?;

    //spawn threading to output the csv quickly
    for (client, account) in accounts {
        output.serialize((
            client,
            format!("{:.4}", account.available),
            format!("{:.4}", account.held),
            format!("{:.4}", account.total),
            account.locked,
        ))?;
    }

    output.flush()?;

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    #[test]
    fn process_transaction_deposit_new_client() {
        //arrange
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();
        let transaction = Transaction::new(
            TxnKind::Deposit, 1, 1, Some(Decimal::new(1003456, 4)));
        
        //act
        process_transaction(&transaction, &mut accounts, &mut transactions);

        //assert

        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(1_003_456, 4));
        assert_eq!(account.total, Decimal::new(1_003_456, 4));
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
        assert!(transactions.contains_key(&1));
    }

    #[test]
    fn process_transaction_deposit_existing_client() {
        //arrange & act
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();

        let transaction1 = Transaction::new(TxnKind::Deposit, 1, 1, Some(Decimal::new(50_0112, 4)));
        process_transaction(&transaction1, &mut accounts, &mut transactions);

        let transaction2 = Transaction::new(TxnKind::Deposit, 1, 2, Some(Decimal::new(100_5674, 4)));
        process_transaction(&transaction2, &mut accounts, &mut transactions);

        //assert
        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(150_5786, 4)); // 150.5789
        assert_eq!(account.total, Decimal::new(150_5786, 4));
        assert_eq!(transactions.len(), 2);
    }

     #[test]
    fn process_transaction_withdraw_new_client() {
        //arrange
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();
        
        //act: Try to withdraw 50.0 from account with zero balance
        let txn = Transaction::new(TxnKind::Withdrawal, 1, 1, Some(Decimal::new(500000, 4)));
        process_transaction(&txn, &mut accounts, &mut transactions);
        
        //assert
        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
        assert!(!transactions.contains_key(&1));
    }

    #[test]
    fn process_transaction_withdraw_existing_client() {
        //arrange
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();
        
        //act
        // First deposit: 100.0
        let deposit = Transaction::new(TxnKind::Deposit, 1, 1, Some(Decimal::new(100_0000, 4)));
        process_transaction(&deposit, &mut accounts, &mut transactions);
        
        // Successful withdrawal: 30.0
        let withdrawal = Transaction::new(TxnKind::Withdrawal, 1, 2, Some(Decimal::new(30_0000, 4)));
        process_transaction(&withdrawal, &mut accounts, &mut transactions);
        
        //assert
        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(70_0000, 4)); // 70.0
        assert_eq!(account.total, Decimal::new(70_0000, 4));
        assert!(transactions.contains_key(&2));
    }

    #[test]
    fn test_dispute_flow() {
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();
        
        // Deposit: 100.0
        let deposit = Transaction::new(TxnKind::Deposit, 1, 1, Some(Decimal::new(100_0000, 4)));
        process_transaction(&deposit, &mut accounts, &mut transactions);
        
        // Dispute the deposit
        let dispute = Transaction::new(TxnKind::Dispute, 1, 1, None);
        process_transaction(&dispute, &mut accounts, &mut transactions);
        
        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::new(100_0000, 4)); // 100.0
        assert_eq!(account.total, Decimal::new(100_0000, 4));
    }

    #[test]
    fn test_chargeback_locks_account() {
        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();
        
        // Deposit: 100.0
        let deposit = Transaction::new(TxnKind::Deposit, 1, 1, Some(Decimal::new(1000000, 4)));
        process_transaction(&deposit, &mut accounts, &mut transactions);
        
        let dispute = Transaction::new(TxnKind::Dispute, 1, 1, None);
        process_transaction(&dispute, &mut accounts, &mut transactions);
        
        let chargeback = Transaction::new(TxnKind::Chargeback, 1, 1, None);
        process_transaction(&chargeback, &mut accounts, &mut transactions);
        
        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
        assert!(account.locked);
    }
}