/*
 * server.rs
 *
 * Implementation of EasyDB database server
 *
 * University of Toronto
 * 2019
 */

// TCP
use std::net::TcpListener;
use std::net::TcpStream;
// IO
use std::io;
use std::io::{
    Error,
    ErrorKind,
    Write,
};
// Multi-threading
use std::sync::Arc;
// From Packet
use packet::Command;
use packet::Response;
use packet::Network;
// From Schema
use schema::Table;
// From Database
use database::handle_request;
use database::Database;

fn single_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    /* 
     * you probably need to use table_schema somewhere here or in
     * Database::new 
     */
    let db = Arc::new(Database::new(table_schema));

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        
        if verbose {
            println!("Connected to {}", stream.peer_addr().unwrap());
        }
        
        match handle_connection(stream, db.clone()) {
            Ok(()) => {
                if verbose {
                    println!("Disconnected.");
                }
            },
            Err(e) => eprintln!("Connection error: {:?}", e),
        };
    }
}

fn multi_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    // Initialize the database object using the specified table schema
    let db = Arc::new(Database::new(table_schema));

    for stream in listener.incoming() {
        let thread_db = db.clone();
        let stream = stream.unwrap();

        if verbose {
            println!("Connected to {}", stream.peer_addr().unwrap());
        }

        std::thread::spawn(move || {
            match handle_connection(stream, thread_db) {
                Ok(()) => {
                    if verbose {
                        println!("Disconnected.");
                    }
                }
                Err(e) => eprintln!("Connection error: {:?}", e),
            };
        });
    }
}

/* Sets up the TCP connection between the database client and server */
pub fn run_server(table_schema: Vec<Table>, ip_address: String, verbose: bool)
{
    let listener = match TcpListener::bind(ip_address) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Could not start server: {}", e);
            return;
        },
    };
    
    println!("Listening: {:?}", listener);
    
    multi_threaded(listener, table_schema, verbose);
}

impl Network for TcpStream {}

/* Receive the request packet from ORM and send a response back */
fn handle_connection(mut stream: TcpStream, db: Arc<Database>)
                     -> io::Result<()> {
    /*
     * Tells the client that the connection to server is successful.
     * TODO: respond with SERVER_BUSY when attempting to accept more than
     *       4 simultaneous clients.
     */
    // No thread is available
    if Arc::strong_count(&db) > 5 {
        stream.respond(&Response::Error(Response::SERVER_BUSY))?;
        return Err(Error::new(ErrorKind::Other, "No thread available."));
    }

    // Tells the client that the connection to server is successful.
    stream.respond(&Response::Connected)?;

    loop {
        let request = match stream.receive() {
            Ok(request) => request,
            Err(e) => {
                /* respond error */
                stream.respond(&Response::Error(Response::BAD_REQUEST))?;
                return Err(e);
            }
        };

        /* we disconnect with client upon receiving Exit */
        if let Command::Exit = request.command {
            break;
        }

        /* Send back a response */
        let response = handle_request(request, &*db);

        stream.respond(&response)?;
        stream.flush()?;
    }

    Ok(())
}


