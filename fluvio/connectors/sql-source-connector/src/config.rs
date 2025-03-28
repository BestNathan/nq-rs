use fluvio_connector_common::connector;

#[connector(config, name = "sql")]
#[derive(Debug)]
pub(crate) struct CustomConfig {
    pub url: String,
    pub query: String,
}
