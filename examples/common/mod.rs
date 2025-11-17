pub mod topics {

    pub const SEND_EMAIL: &str = "send.email";
    pub const RECEIVE_EMAIL: &str = "receive.email";
}

pub mod payloads {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Email {
        pub send_to: String,

        pub message: String,
    }

    #[allow(dead_code)]
    impl Email {
        pub fn new(send_to: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                send_to: send_to.into(),
                message: message.into(),
            }
        }

        pub fn random() -> Self {
            let receivers = ["alice", "bob", "charlie", "dave", "eve"];
            let send_to = receivers[rand::random_range(0..receivers.len())];

            let messages = [
                "Hi, how are you?",
                "Hey, what's up?",
                "Hello, what's going on?",
                "Hola, como estas?",
            ];
            let message = messages[rand::random_range(0..messages.len())];

            Self::new(send_to, message)
        }
    }
}
