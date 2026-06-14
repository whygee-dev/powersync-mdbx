use std::{collections::HashMap, env, io, net::SocketAddr};

use tokio::net::{TcpListener, TcpSocket};

const TCP_BACKLOG_ENV: &str = "POWERSYNC_RUST_TCP_BACKLOG";
const TCP_NODELAY_ENV: &str = "POWERSYNC_RUST_TCP_NODELAY";
const TCP_REUSEADDR_ENV: &str = "POWERSYNC_RUST_TCP_REUSEADDR";
#[cfg(all(
    unix,
    not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
))]
const TCP_REUSEPORT_ENV: &str = "POWERSYNC_RUST_TCP_REUSEPORT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerOptions {
    pub backlog: u32,
    pub nodelay: bool,
    pub reuseaddr: bool,
    #[cfg(all(
        unix,
        not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
    ))]
    pub reuseport: bool,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            backlog: 1024,
            nodelay: true,
            reuseaddr: !cfg!(windows),
            #[cfg(all(
                unix,
                not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
            ))]
            reuseport: false,
        }
    }
}

impl ListenerOptions {
    pub fn from_env() -> io::Result<Self> {
        let vars = env::vars().collect::<HashMap<_, _>>();
        Self::from_lookup(|name| vars.get(name).cloned())
    }

    fn from_lookup<F>(lookup: F) -> io::Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut options = Self::default();
        options.backlog = parse_u32_env(&lookup, TCP_BACKLOG_ENV, options.backlog)?;
        options.nodelay = parse_bool_env(&lookup, TCP_NODELAY_ENV, options.nodelay)?;
        options.reuseaddr = parse_bool_env(&lookup, TCP_REUSEADDR_ENV, options.reuseaddr)?;
        #[cfg(all(
            unix,
            not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
        ))]
        {
            options.reuseport = parse_bool_env(&lookup, TCP_REUSEPORT_ENV, options.reuseport)?;
        }
        Ok(options)
    }
}

pub fn bind_listener(addr: SocketAddr, options: ListenerOptions) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    if options.reuseaddr {
        socket.set_reuseaddr(true)?;
    }
    if options.nodelay {
        socket.set_nodelay(true)?;
    }
    #[cfg(all(
        unix,
        not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
    ))]
    if options.reuseport {
        socket.set_reuseport(true)?;
    }

    socket.bind(addr)?;
    socket.listen(options.backlog)
}

fn parse_bool_env<F>(lookup: &F, name: &str, default: bool) -> io::Result<bool>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        None => Ok(default),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid boolean for {name}: {value}"),
            )),
        },
    }
}

fn parse_u32_env<F>(lookup: &F, name: &str, default: u32) -> io::Result<u32>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        None => Ok(default),
        Some(value) => value.parse::<u32>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid unsigned integer for {name}: {value} ({error})"),
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::ListenerOptions;
    use std::collections::HashMap;

    #[test]
    fn listener_options_have_expected_transport_defaults() {
        let options = ListenerOptions::default();

        assert_eq!(options.backlog, 1024);
        assert!(options.nodelay);
        assert_eq!(options.reuseaddr, !cfg!(windows));
        #[cfg(all(
            unix,
            not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
        ))]
        assert!(!options.reuseport);
    }

    #[test]
    fn listener_options_parse_env_overrides() {
        let vars = HashMap::from([
            ("POWERSYNC_RUST_TCP_BACKLOG".to_string(), "2048".to_string()),
            (
                "POWERSYNC_RUST_TCP_NODELAY".to_string(),
                "false".to_string(),
            ),
            (
                "POWERSYNC_RUST_TCP_REUSEADDR".to_string(),
                "true".to_string(),
            ),
        ]);

        let options = ListenerOptions::from_lookup(|name| vars.get(name).cloned()).unwrap();

        assert_eq!(options.backlog, 2048);
        assert!(!options.nodelay);
        assert!(options.reuseaddr);
    }
}
