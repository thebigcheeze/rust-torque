extern crate tiny_http;
extern crate postgres;

use postgres::{PostgresConnection, NoSsl};

struct Parameter {
    name: String,
    value: String
}

enum ParserState {
    PreParse,
    ParsingName,
    ParsingValue
}

fn parse_params(input: &str) -> Vec<Parameter> {
    let mut ret_val = Vec::<Parameter>::new();
    
    let mut name : String = String::new();
    let mut val : String = String::new();
    
    let mut cur : ParserState = PreParse;
    
    for c in input.chars() {
        match (cur, c) {
            (PreParse, n) if n != '?' => {continue;},
            (PreParse, _) => {
                cur = ParsingName;
            },
            (ParsingName, '=') => {
                cur = ParsingValue;
            },
            (ParsingName, n) => {
                name.push(n);
            },
            (ParsingValue, '&') => {
                ret_val.push(Parameter{name:name, value:val});
                name = String::new();
                val = String::new();
                cur = ParsingName;
            },
            (ParsingValue, n) => {
                val.push(n);
            }
        }
    }
    
    if match cur { ParsingValue => true, _ => false} {
        ret_val.push(Parameter{name:name, value:val});
    }

    ret_val
}


fn pg_connect() -> PostgresConnection {
    let conn_str = "postgres://torque:torque@192.168.1.217/torque";
    PostgresConnection::connect(conn_str, &NoSsl).unwrap()
}

fn main() {
    println!("Beginning Torque Broker.");

    let port = 9500u16;

    let server = tiny_http::ServerBuilder::new().with_port(port).build().unwrap();
     
    println!("hosting server on port {}", port);

    let db = pg_connect();

    spawn(proc() {for request in server.incoming_requests() {
        let good_response = tiny_http::Response::from_string("OK!".to_string());
        let bad_response = tiny_http::Response::from_string("BAD!".to_string());

        println!("Got request: {}", request.get_url());
        let parsed_params = parse_params(request.get_url());
        
        let mut session = -1i64;
        let mut time = -1i64;
        
        // I need to find the session and the time
        for param in parsed_params.iter() {
            println!("{} - {}", param.name, param.value);

            if param.name.as_slice() == "session" {
                match from_str::<i64>(param.value.as_slice()) {
                    Some(n) => session = n,
                    None => {
                        println!("Non-parsable session query parameter: {}", param.value);
                    }
                }
                continue;
            }

            if param.name.as_slice() == "time" {
                match from_str::<i64>(param.value.as_slice()) {
                    Some(n) => time = n,
                    None => {
                        println!("Non-parsable time query parameter: {}", param.value);
                    }
                }
                continue;
            }
        }

        if session < 0 {
            println!("No session found.");
            request.respond(bad_response);
            continue;
        }

        if time < 0 {
            println!("No time found.");
            request.respond(bad_response);
            continue;
        }

        let event_insert = db.prepare("insert into event (session, time) values ($1, $2) returning eventid").unwrap();

        let mut event_id = -1i64;
        
        for row in event_insert.query(&[&session, &time]).unwrap() {
            event_id = row.get(0u);
        }
        
        if event_id < 0i64 {
            println!("event id did not return a valid value: {}", event_id);
            request.respond(bad_response);
            continue;
        }

        for param in parsed_params.iter() {
            if param.name.as_slice().char_at(0) == 'k' {
                db.execute(format!("INSERT INTO event_data (eventid, name, value) VALUES 
                              ($1, $2, (cast({} as numeric(30,20))))", param.value).as_slice(), 
                              &[&event_id, &param.name]).unwrap();
            }
        }

        request.respond(good_response);
    }});

    println!("Setup complete!");
}
