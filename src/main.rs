#![allow(dead_code)]
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};
use csv::ReaderBuilder;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::env;
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
async fn main() {

    //1. Setup channel
    let (tx, mut rx) = mpsc::channel::<Transaction>(100_000);

    //2. Set up TCP listener FIRST
    let listener = TcpListener::bind("127.0.0.1:8080").await.expect("Failed to bind");
    println!("Server listening on 127.0.0.1:8080...");

    //3. Consumer task - processes all transactions and maintains a global state
    tokio::spawn(async move {
        //declare global accounts, transaction
        let mut accounts: HashMap<u16, Account> =  HashMap::new();
        let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();

        while let Some(transaction) = rx.recv().await {
            let client = transaction.client;
            if !accounts.contains_key(&client) {
                accounts.insert(client, Account::new());
                println!("Consumer: New client #{}", client) ;
            }

            if let Some(account) = accounts.get_mut(&client) {
                //skip processing if account is locked
                if account.locked {
                    continue;
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

        //here need to output shit to csv
        let mut output = csv::Writer::from_writer(io::stdout());

        //need to write header manually still
        output.write_record(&["client", "available", "held", "total", "locked"]).expect("Failed to write header");

        //spawn threading to output the csv quickly
        for (client, account) in &accounts {
            output.serialize((
                client,
                format!("{:.4}", account.available),
                format!("{:.4}", account.held),
                format!("{:.4}", account.total),
                account.locked,
            )).expect("Failed to write account record");
        }

        output.flush().expect("Failed to flush writer");
    });

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

