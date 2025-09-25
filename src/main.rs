#![allow(dead_code)]
use std::collections::HashMap;
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

#[derive(Debug, PartialEq)]
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

fn main() {

    let args: Vec<String> = env::args().collect();
    
    let transactions_file: &String = &args[1];

    println!("Reading file: {}", transactions_file);

    let mut accounts: HashMap<u16, Account> =  HashMap::new();

    let mut transactions: HashMap<u32, (TxnKind, Decimal, TxnStatus)> = HashMap::new();


    let mut rdr = ReaderBuilder::new().trim(csv::Trim::All)
                                    .from_path(transactions_file)
                                    .expect("Failed to open CSV file");

    for record in rdr.deserialize::<Transaction>() {
        match record {
            Ok(transaction) => {
                let client: u16 = transaction.client;
                let client_exist = accounts.contains_key(&client); 
                
                if !client_exist {
                    accounts.insert(transaction.client, Account::new());
                    println!("Inserting a new client # {}", client);
                }
                
                if let Some(account) = accounts.get_mut(&transaction.client) {

                    if account.locked {
                        println!("Account is frozen, sorry – not processing further!");
                        continue;
                    }
                    println!("Processing {:?} for {:?}", transaction, account);

                    match transaction.kind {
                        TxnKind::Deposit => {
                            account.available += transaction.amount;
                            account.total += transaction.amount;
                            transactions.insert(transaction.tx, (transaction.kind, transaction.amount, TxnStatus::Okay));
                        }
                        TxnKind::Withdrawal => {
                            if account.available >= transaction.amount {
                                account.available -= transaction.amount;
                                account.total -= transaction.amount;
                                transactions.insert(transaction.tx, (transaction.kind, transaction.amount, TxnStatus::Okay));
                            }
                        }
                        TxnKind::Dispute => {
                            if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                                account.held +=  txn_record.1;
                                account.available -= txn_record.1;
                                txn_record.2 = TxnStatus::Disputed;
                            }
                        }
                        TxnKind::Resolve => {
                            if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                                if txn_record.2 == TxnStatus::Disputed {
                                    account.held -=  txn_record.1;
                                    account.available += txn_record.1;
                                    txn_record.2 = TxnStatus::Okay;
                                }
                            }
                        }
                        TxnKind::Chargeback => {
                            if let Some(txn_record) = transactions.get_mut(&transaction.tx) {
                                if txn_record.2 == TxnStatus::Disputed {
                                    account.held -=  txn_record.1;
                                    account.total -= txn_record.1;
                                    account.locked = true;
                                }
                            }
                        }
                    }
                }
                
            }
            Err(e) => eprintln!("Bad row: {}", e),
        }
    }
    //here need to output shit to csv
    let mut output = csv::Writer::from_writer(io::stdout());

    //need to write header manually still
    output.write_record(&["client", "available", "held", "total", "locked"]).expect("Failed to write header");

    for (client, account) in &accounts {
        output.serialize((
            client,
            account.available,
            account.held,
            account.total,
            account.locked,
        )).expect("Failed to write account record");
    }

    output.flush().expect("Failed to flush writer");

}

