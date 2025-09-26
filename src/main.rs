#![allow(dead_code)]
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use std::{collections::HashMap, env};
use tokio::io::{AsyncBufReadExt, BufReader};
use csv::ReaderBuilder;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Deserialize, Serialize)]
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
    kind: TxnKind,
    client: u16,
    tx: u32,
    amount: Decimal,
}

impl Transaction {
    pub fn new(kind: TxnKind, client: u16, tx: u32, amount: Decimal) -> Transaction {
        Transaction { kind, client, tx, amount}
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Account {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
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

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        //CLI mode
        let transaction_file = &args[1];
        process_single_file(transaction_file)?;
    } else {
        //TCP server mode - extension for concurrent processing
        println!("Starting TCP server mode..");
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

async fn start_tcp_server() -> Result<(), Box<dyn std::error::Error>> {
    // Setup channel for transactions
    let (tx, mut rx) = mpsc::channel::<Transaction>(100_000);

    // Set up TCP listener
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("TCP server listening on 127.0.0.1:8080...");

    // Single Consumer which adjusts accounts and maintains global state
    tokio::spawn(async move {
        //declare global accounts, transaction
        let mut accounts: HashMap<u16, Account> =  HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();

        while let Some(transaction) = rx.recv().await {
            process_transaction(&transaction, &mut accounts, &mut transactions);
        }

    });

    //Multiple producers which reads transaction and send to consumer for processing
    loop {
        let (socket, addr) = listener.accept().await.expect("Failed to accept connection");
        println!("New connection from: {}", addr);

        let sender = tx.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(socket);
            let mut file_path = String::new();

            if let Ok(_) = reader.read_line(&mut file_path).await {
                let file_path = file_path.trim();
                println!("Producer {}: Processing CSV file: {}", addr, file_path);

                // Open and stream CSV file
                match ReaderBuilder::new().trim(csv::Trim::All).from_path(file_path) {
                    Ok(mut rdr) => {
                        // Send each transaction to consumer
                        for record in rdr.deserialize::<Transaction>() {
                            match record {
                                Ok(transaction) => {
                                    if let Err(e) = sender.send(transaction).await {
                                        eprintln!("Failed to send transaction: {}", e);
                                        break;
                                    }
                                }
                                Err(err) => {
                                    eprintln!("Error parsing record from {}: {}", file_path, err);
                                }
                            }
                        }
                        println!("Producer {}: Finished processing {}", addr, file_path);
                    }
                    Err(err) => {
                        eprintln!("Failed to open CSV file {}: {}", file_path, err);
                    }
                }
            }
        });
    }
}

fn process_transaction(transaction: &Transaction, accounts: &mut HashMap<u16, Account>, transactions: &mut HashMap<u32, (TxnKind, Decimal, TxnStatus)>) {

    let client = transaction.client;
    if !accounts.contains_key(&client) {
        accounts.insert(client, Account::new());
        println!("Consumer: New client #{}", client) ;
    }

    if let Some(account) = accounts.get_mut(&client) {
        //skip processing if account is locked
        if account.locked {
            return;
        }

        match transaction.kind {
            TxnKind::Deposit => {
                account.available += transaction.amount;
                account.total += transaction.amount;
            },
            TxnKind::Withdrawal => {
                if account.available >= transaction.amount {
                        account.available -= transaction.amount;
                        account.total -= transaction.amount;
                }
            },
            TxnKind::Dispute => {
                if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                        account.held +=  txn_record.1;
                        account.available -= txn_record.1;
                        txn_record.2 = TxnStatus::Disputed;
                }
            },
            TxnKind::Resolve => {
                if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                        if txn_record.2 == TxnStatus::Disputed {
                            account.held -=  txn_record.1;
                            account.available += txn_record.1;
                            txn_record.2 = TxnStatus::Okay;
                        }
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

