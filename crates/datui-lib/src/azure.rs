//! Azure Blob Storage: logins, finding storage accounts, and listing what is in them.
//!
//! A signed-in `az` is asked for tokens, as object_store itself does, rather than its
//! token cache being read: the cache is an internal format, encrypted to the user on
//! Windows, and asking keeps MFA and conditional access working without datui knowing
//! about either. One Resource Graph query finds every storage account the login can
//! see across its subscriptions, so nobody has to name an account to browse it.
//!
//! Everything here that touches the network or runs `az` blocks, and is only called
//! from a worker.

use crate::cloud_browse::Environment;
use crate::cloud_command::CommandError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

/// The scope of a token for reading blobs.
pub const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
/// The scope of a token for Resource Graph, which finds storage accounts.
pub const MANAGEMENT_SCOPE: &str = "https://management.azure.com/.default";
/// Sent on every request. Without it, anonymous requests are refused with
/// `FeatureVersionMismatch`, and newer response fields are left out.
pub const API_VERSION: &str = "2023-11-03";

/// How a request to one account is authorized.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum AzureAuth {
    /// Nothing known yet.
    #[default]
    None,
    /// A signed-in `az`, asked for a token when one is needed.
    AzCli,
    /// An Entra ID token.
    Bearer(String),
    /// The account's shared key.
    Key(String),
    /// A shared access signature, without the leading `?`.
    Sas(String),
}

/// How to reach Azure Blob Storage for one source.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AzureSettings {
    /// The one account a key, SAS or connection string names. A signed-in identity
    /// reaches every account it can see, so it has none.
    pub account: Option<String>,
    pub auth: AzureAuth,
    /// A blob endpoint other than `https://<account>.blob.core.windows.net/`.
    pub blob_endpoint: Option<String>,
    /// Azurite, the local emulator.
    pub use_emulator: bool,
}

impl AzureSettings {
    /// The blob service endpoint for `account`, ending in `/`.
    pub fn blob_endpoint_for(&self, account: &str) -> String {
        match (&self.blob_endpoint, self.use_emulator) {
            (Some(endpoint), _) => format!("{}/", endpoint.trim_end_matches('/')),
            (None, true) => format!("http://127.0.0.1:10000/{account}/"),
            (None, false) => format!("https://{account}.blob.core.windows.net/"),
        }
    }

    /// These settings with a token in place of `AzCli`. Runs `az`.
    pub fn with_token(mut self, env: &Environment<'_>) -> Result<Self, String> {
        if self.auth == AzureAuth::AzCli {
            self.auth = AzureAuth::Bearer(token(STORAGE_SCOPE, env)?);
        }
        Ok(self)
    }
}

/// The Azurite account and key everyone uses; they are published in its documentation.
const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// The settings a connection string describes: `AccountName`, `AccountKey`,
/// `SharedAccessSignature`, `BlobEndpoint`, `EndpointSuffix`,
/// `DefaultEndpointsProtocol`, or `UseDevelopmentStorage=true` for Azurite.
pub fn parse_connection_string(text: &str) -> Option<AzureSettings> {
    let pairs: HashMap<String, String> = text
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    if pairs
        .get("usedevelopmentstorage")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        return Some(AzureSettings {
            account: Some(AZURITE_ACCOUNT.to_string()),
            auth: AzureAuth::Key(AZURITE_KEY.to_string()),
            blob_endpoint: None,
            use_emulator: true,
        });
    }
    let account = pairs.get("accountname").cloned();
    let auth = match pairs.get("accountkey") {
        Some(key) => AzureAuth::Key(key.clone()),
        None => AzureAuth::Sas(
            pairs
                .get("sharedaccesssignature")?
                .trim_start_matches('?')
                .to_string(),
        ),
    };
    let blob_endpoint = pairs.get("blobendpoint").cloned().or_else(|| {
        let account = account.as_ref()?;
        let suffix = pairs.get("endpointsuffix")?;
        let protocol = pairs
            .get("defaultendpointsprotocol")
            .map(String::as_str)
            .unwrap_or("https");
        Some(format!("{protocol}://{account}.blob.{suffix}"))
    });
    // An account can be left out when the blob endpoint names it.
    let account = account.or_else(|| {
        let endpoint = blob_endpoint.as_deref()?;
        let host = endpoint.split_once("://")?.1.split('/').next()?;
        host.split_once(".blob.").map(|(a, _)| a.to_string())
    })?;
    Some(AzureSettings {
        account: Some(account),
        auth,
        blob_endpoint,
        use_emulator: false,
    })
}

/// What the environment says about Azure, if anything: a connection string, or an
/// account with a key or SAS token.
pub fn from_environment(var: &dyn Fn(&str) -> Option<String>) -> Option<(AzureSettings, String)> {
    let set = |key: &str| {
        var(key)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    if let Some(text) = set("AZURE_STORAGE_CONNECTION_STRING") {
        return parse_connection_string(&text)
            .map(|s| (s, "AZURE_STORAGE_CONNECTION_STRING".to_string()));
    }
    let account = set("AZURE_STORAGE_ACCOUNT_NAME")?;
    let (auth, origin) = match set("AZURE_STORAGE_ACCOUNT_KEY")
        .or_else(|| set("AZURE_STORAGE_ACCESS_KEY"))
    {
        Some(key) => (AzureAuth::Key(key), "AZURE_STORAGE_ACCOUNT_KEY"),
        None => {
            let sas = set("AZURE_STORAGE_SAS_TOKEN").or_else(|| set("AZURE_STORAGE_SAS_KEY"))?;
            (
                AzureAuth::Sas(sas.trim_start_matches('?').to_string()),
                "AZURE_STORAGE_SAS_TOKEN",
            )
        }
    };
    Some((
        AzureSettings {
            account: Some(account),
            auth,
            blob_endpoint: None,
            use_emulator: false,
        },
        origin.to_string(),
    ))
}

/// Whether `az` has been used on this machine: its config folder exists. Not proof the
/// login is still good, which only asking can tell.
pub fn az_login_evidence(env: &Environment<'_>) -> bool {
    let dir = match (env.var)("AZURE_CONFIG_DIR").filter(|v| !v.trim().is_empty()) {
        Some(dir) => PathBuf::from(dir.trim()),
        None => match &env.home {
            Some(home) => home.join(".azure"),
            None => return false,
        },
    };
    (env.exists)(&dir)
}

/// Tokens by scope, kept until shortly before they expire so `az`, which is slow to
/// start, runs once an hour rather than once per request.
type TokenCache = Mutex<HashMap<String, (String, Option<SystemTime>)>>;

fn tokens() -> &'static TokenCache {
    static TOKENS: OnceLock<TokenCache> = OnceLock::new();
    TOKENS.get_or_init(Default::default)
}

/// A token for `scope` from the signed-in `az`.
pub fn token(scope: &str, env: &Environment<'_>) -> Result<String, String> {
    if let Some((token, _)) = tokens().lock().ok().and_then(|t| {
        t.get(scope).cloned().filter(|(_, expires)| {
            expires.is_none_or(|at| at > SystemTime::now() + Duration::from_secs(5 * 60))
        })
    }) {
        return Ok(token);
    }
    let output = (env.run)(
        "az",
        &[
            "account",
            "get-access-token",
            "--scope",
            scope,
            "--output",
            "json",
        ],
    )
    .map_err(|e| match e {
        CommandError::Missing(_) => "needs the Azure CLI".to_string(),
        CommandError::Failed(message) if message.contains("az login") => {
            format!("not logged in: run az login ({})", message.trim())
        }
        other => other.to_string(),
    })?;
    let (token, expires) =
        parse_token(&output).ok_or_else(|| "az returned no token".to_string())?;
    if let Ok(mut cached) = tokens().lock() {
        cached.insert(scope.to_string(), (token.clone(), expires));
    }
    Ok(token)
}

/// The token and expiry from `az account get-access-token --output json`. Newer `az`
/// gives `expires_on` in seconds; older gives only `expiresOn` in local time, which is
/// not worth guessing at, so such a token is refreshed on the next request.
pub fn parse_token(text: &str) -> Option<(String, Option<SystemTime>)> {
    let value: serde_json::Value = serde_json::from_str(text.trim()).ok()?;
    let token = value.get("accessToken")?.as_str()?.to_string();
    if token.is_empty() {
        return None;
    }
    let expires = value
        .get("expires_on")
        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
        .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs));
    Some((token, expires))
}

/// One storage account, as Resource Graph describes it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Account {
    pub name: String,
    pub subscription: Option<String>,
    pub location: Option<String>,
    pub hierarchical_namespace: bool,
    pub blob_endpoint: Option<String>,
    /// Public network access is disabled, or the firewall denies by default.
    pub private_network: bool,
    pub shared_key_access: bool,
}

/// The Resource Graph query for storage accounts, with each one's subscription name.
const ACCOUNTS_QUERY: &str = "resources \
| where type =~ 'microsoft.storage/storageaccounts' \
| join kind=leftouter (resourcecontainers \
    | where type =~ 'microsoft.resources/subscriptions' \
    | project subscriptionId, subscription=name) on subscriptionId \
| project id, name, subscription, location, \
    hns=properties.isHnsEnabled, \
    blob=properties.primaryEndpoints.blob, \
    publicNetworkAccess=properties.publicNetworkAccess, \
    defaultAction=properties.networkAcls.defaultAction, \
    sharedKey=properties.allowSharedKeyAccess \
| order by name asc";

/// A Resource Graph response has more than this many pages only for a tenant nobody
/// browses by scrolling.
const MAX_ACCOUNT_PAGES: usize = 20;

/// Every storage account the signed-in identity can see, across its subscriptions.
pub fn discover_accounts(env: &Environment<'_>) -> Result<Vec<Account>, String> {
    let token = token(MANAGEMENT_SCOPE, env)?;
    let mut accounts = Vec::new();
    let mut skip_token: Option<String> = None;
    for _ in 0..MAX_ACCOUNT_PAGES {
        let mut options = serde_json::json!({ "$top": 1000 });
        if let Some(skip) = &skip_token {
            options["$skipToken"] = serde_json::Value::String(skip.clone());
        }
        let body = serde_json::json!({ "query": ACCOUNTS_QUERY, "options": options });
        let text = crate::cloud_browse::http_agent()
            .post("https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01")
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .send(body.to_string())
            .map_err(|e| format!("{e}"))?
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("could not read the response: {e}"))?;
        let (page, next) = parse_accounts(&text)?;
        accounts.extend(page);
        match next {
            Some(next) => skip_token = Some(next),
            None => break,
        }
    }
    Ok(accounts)
}

/// The accounts in one Resource Graph response, and the token for the next page.
pub fn parse_accounts(text: &str) -> Result<(Vec<Account>, Option<String>), String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("not JSON: {e}"))?;
    if let Some(message) = value
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        return Err(message.to_string());
    }
    let rows = value
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| "no data in the response".to_string())?;
    let text_of = |row: &serde_json::Value, key: &str| {
        row.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|v| !v.is_empty())
    };
    let accounts = rows
        .iter()
        .filter_map(|row| {
            Some(Account {
                name: text_of(row, "name")?,
                subscription: text_of(row, "subscription"),
                location: text_of(row, "location"),
                hierarchical_namespace: row.get("hns").and_then(|v| v.as_bool()) == Some(true),
                blob_endpoint: text_of(row, "blob"),
                private_network: text_of(row, "publicNetworkAccess").as_deref() == Some("Disabled")
                    || text_of(row, "defaultAction").as_deref() == Some("Deny"),
                // Unset means allowed.
                shared_key_access: row.get("sharedKey").and_then(|v| v.as_bool()) != Some(false),
            })
        })
        .collect();
    let next = value
        .get("$skipToken")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Ok((accounts, next))
}

/// Containers in one account. `settings` must already hold a token or key, not `AzCli`.
pub fn list_containers(account: &str, settings: &AzureSettings) -> Result<Vec<String>, String> {
    let endpoint = settings.blob_endpoint_for(account);
    let mut names = Vec::new();
    let mut marker: Option<String> = None;
    for _ in 0..MAX_ACCOUNT_PAGES {
        let mut url = format!("{endpoint}?comp=list&maxresults=5000");
        if let Some(marker) = &marker {
            url.push_str(&format!(
                "&marker={}",
                crate::cloud_browse::urlencode(marker)
            ));
        }
        let text = send_signed(&url, account, settings)?;
        let (page, next) = parse_containers(&text)?;
        names.extend(page);
        match next {
            Some(next) => marker = Some(next),
            None => break,
        }
    }
    names.sort();
    Ok(names)
}

/// A signed GET, returning the body of a successful response and the service's own
/// error code and message otherwise.
fn send_signed(url: &str, account: &str, settings: &AzureSettings) -> Result<String, String> {
    let url = match &settings.auth {
        AzureAuth::Sas(sas) => format!("{url}&{sas}"),
        _ => url.to_string(),
    };
    let mut request = http::Request::builder()
        .method("GET")
        .uri(&url)
        .header("x-ms-version", API_VERSION)
        .header(
            "x-ms-date",
            chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        )
        .body(object_store::client::HttpRequestBody::empty())
        .map_err(|e| format!("could not build the request: {e}"))?;
    match &settings.auth {
        AzureAuth::Bearer(token) => {
            let credential = object_store::azure::AzureCredential::BearerToken(token.clone());
            object_store::azure::AzureAuthorizer::new(&credential, account).authorize(&mut request);
        }
        AzureAuth::Key(key) => {
            let key = object_store::azure::AzureAccessKey::try_new(key)
                .map_err(|e| format!("the account key is not valid: {e}"))?;
            let credential = object_store::azure::AzureCredential::AccessKey(key);
            object_store::azure::AzureAuthorizer::new(&credential, account).authorize(&mut request);
        }
        AzureAuth::Sas(_) | AzureAuth::None | AzureAuth::AzCli => {}
    }
    let mut call = crate::cloud_browse::http_agent()
        .get(&url)
        .config()
        .http_status_as_error(false)
        .build();
    for (name, value) in request.headers() {
        if let Ok(value) = value.to_str() {
            call = call.header(name.as_str(), value);
        }
    }
    let mut response = call.call().map_err(|e| format!("{e}"))?;
    let status = response.status();
    let text = response
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("could not read the response: {e}"))?;
    if status.is_success() {
        Ok(text)
    } else {
        Err(describe_error(status.as_u16(), &text))
    }
}

/// `403 AuthorizationPermissionMismatch: ...` from an error document, with what fixes
/// the one that trips up people who manage their storage in the Portal.
pub fn describe_error(status: u16, body: &str) -> String {
    let field = |name: &str| {
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        let start = body.find(&open)? + open.len();
        let end = body[start..].find(&close)? + start;
        Some(body[start..end].trim().to_string())
    };
    let code = field("Code").unwrap_or_default();
    let message = field("Message")
        .and_then(|m| m.lines().next().map(str::to_string))
        .unwrap_or_default();
    let mut text = format!("{status} {code}");
    if !message.is_empty() {
        text.push_str(&format!(": {message}"));
    }
    if code == "AuthorizationPermissionMismatch" {
        text.push_str(
            ". Reading blobs with a sign-in needs the Storage Blob Data Reader role on the \
             account, or an ACL on the path when it has hierarchical namespace",
        );
    }
    text
}

/// Container names from a `List Containers` response, and the marker for the next page.
pub fn parse_containers(text: &str) -> Result<(Vec<String>, Option<String>), String> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("<Container>") {
        rest = &rest[start + "<Container>".len()..];
        let end = rest.find("</Container>").unwrap_or(rest.len());
        let block = &rest[..end];
        if let Some(name_start) = block.find("<Name>") {
            let after = &block[name_start + "<Name>".len()..];
            if let Some(name_end) = after.find("</Name>") {
                let name = after[..name_end].trim();
                if !name.is_empty() {
                    names.push(name.to_string());
                }
            }
        }
        rest = &rest[end..];
    }
    if names.is_empty() && !text.contains("<EnumerationResults") {
        return Err("not a container listing".to_string());
    }
    let next = text
        .find("<NextMarker>")
        .and_then(|start| {
            let after = &text[start + "<NextMarker>".len()..];
            after
                .find("</NextMarker>")
                .map(|end| after[..end].trim().to_string())
        })
        .filter(|m| !m.is_empty());
    Ok((names, next))
}

/// An object store for one container. `settings` must already hold a token or key.
pub fn store(
    account: &str,
    container: &str,
    settings: &AzureSettings,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, String> {
    let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container);
    if settings.use_emulator {
        builder = builder.with_use_emulator(true);
    }
    if let Some(endpoint) = &settings.blob_endpoint {
        builder = builder
            .with_endpoint(endpoint.trim_end_matches('/').to_string())
            .with_allow_http(endpoint.starts_with("http://"));
    }
    builder = match &settings.auth {
        AzureAuth::Bearer(token) => builder.with_bearer_token_authorization(token.clone()),
        AzureAuth::Key(key) => builder.with_access_key(key.clone()),
        AzureAuth::Sas(sas) => {
            builder.with_config(object_store::azure::AzureConfigKey::SasKey, sas.clone())
        }
        AzureAuth::None | AzureAuth::AzCli => builder.with_skip_signature(true),
    };
    let store = builder
        .build()
        .map_err(|e| format!("Azure is not configured: {e}"))?;
    Ok(std::sync::Arc::new(store))
}

/// Polars' view of the same settings, for `scan_parquet` on an `abfss://` URL.
pub fn polars_options(
    account: &str,
    settings: &AzureSettings,
) -> Vec<(object_store::azure::AzureConfigKey, String)> {
    use object_store::azure::AzureConfigKey;
    let mut options = vec![(AzureConfigKey::AccountName, account.to_string())];
    if settings.use_emulator {
        options.push((AzureConfigKey::UseEmulator, "true".to_string()));
    }
    if let Some(endpoint) = &settings.blob_endpoint {
        options.push((AzureConfigKey::Endpoint, endpoint.clone()));
    }
    match &settings.auth {
        AzureAuth::Bearer(token) => options.push((AzureConfigKey::Token, token.clone())),
        AzureAuth::Key(key) => options.push((AzureConfigKey::AccessKey, key.clone())),
        AzureAuth::Sas(sas) => options.push((AzureConfigKey::SasKey, sas.clone())),
        AzureAuth::None | AzureAuth::AzCli => {
            options.push((AzureConfigKey::SkipSignature, "true".to_string()))
        }
    }
    options
}

/// Whether a listed object is only a folder marker. Accounts with hierarchical
/// namespace list every folder twice, once as a prefix and once as an empty blob of
/// the same name; the blob is not data.
pub fn is_folder_marker(name: &str, size: u64, prefixes: &[String]) -> bool {
    size == 0 && prefixes.iter().any(|p| p.trim_end_matches('/') == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_strings_in_their_common_shapes() {
        let key = parse_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=datalake001;AccountKey=a2V5;EndpointSuffix=core.windows.net",
        )
        .unwrap();
        assert_eq!(key.account.as_deref(), Some("datalake001"));
        assert_eq!(key.auth, AzureAuth::Key("a2V5".to_string()));
        assert_eq!(
            key.blob_endpoint.as_deref(),
            Some("https://datalake001.blob.core.windows.net")
        );

        let sas = parse_connection_string(
            "BlobEndpoint=https://datalake001.blob.core.windows.net/;SharedAccessSignature=sv=2022-11-02&sig=abc",
        )
        .unwrap();
        assert_eq!(sas.account.as_deref(), Some("datalake001"));
        assert_eq!(
            sas.auth,
            AzureAuth::Sas("sv=2022-11-02&sig=abc".to_string())
        );

        let azurite = parse_connection_string("UseDevelopmentStorage=true").unwrap();
        assert!(azurite.use_emulator);
        assert_eq!(
            azurite.blob_endpoint_for("devstoreaccount1"),
            "http://127.0.0.1:10000/devstoreaccount1/"
        );

        assert!(
            parse_connection_string("AccountName=x").is_none(),
            "no key or SAS"
        );
    }

    #[test]
    fn the_environment_names_an_account_with_a_key_or_sas() {
        let vars = |pairs: &'static [(&'static str, &'static str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.to_string())
            }
        };
        let (settings, origin) = from_environment(&vars(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_SAS_TOKEN", "?sv=1&sig=2"),
        ]))
        .unwrap();
        assert_eq!(settings.auth, AzureAuth::Sas("sv=1&sig=2".to_string()));
        assert_eq!(origin, "AZURE_STORAGE_SAS_TOKEN");
        assert!(from_environment(&vars(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")])).is_none());
    }

    #[test]
    fn a_token_with_and_without_an_expiry() {
        let (token, expires) = parse_token(
            r#"{"accessToken": "eyJ0", "expiresOn": "2026-09-16 16:32:37.000000", "expires_on": 1789600000, "tokenType": "Bearer"}"#,
        )
        .unwrap();
        assert_eq!(token, "eyJ0");
        assert_eq!(
            expires,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_789_600_000))
        );
        let (_, expires) =
            parse_token(r#"{"accessToken": "eyJ0", "expiresOn": "2026-09-16 16:32:37.000000"}"#)
                .unwrap();
        assert_eq!(expires, None);
        assert!(parse_token(r#"{"accessToken": ""}"#).is_none());
    }

    #[test]
    fn accounts_from_resource_graph() {
        let (accounts, next) = parse_accounts(
            r#"{"totalRecords": 2, "$skipToken": "page2", "data": [
                {"name": "datalake001", "subscription": "Azure subscription 1", "location": "eastus",
                 "hns": true, "blob": "https://datalake001.blob.core.windows.net/",
                 "publicNetworkAccess": "Enabled", "defaultAction": "Allow", "sharedKey": null},
                {"name": "locked001", "hns": false, "publicNetworkAccess": "Disabled", "sharedKey": false}
            ]}"#,
        )
        .unwrap();
        assert_eq!(next.as_deref(), Some("page2"));
        assert_eq!(accounts[0].name, "datalake001");
        assert!(accounts[0].hierarchical_namespace && accounts[0].shared_key_access);
        assert!(!accounts[0].private_network);
        assert!(accounts[1].private_network && !accounts[1].shared_key_access);

        let err = parse_accounts(r#"{"error": {"code": "AuthorizationFailed", "message": "no"}}"#)
            .unwrap_err();
        assert_eq!(err, "no");
    }

    #[test]
    fn containers_and_the_next_page() {
        let (names, next) = parse_containers(
            r#"<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="https://a.blob.core.windows.net/"><Containers><Container><Name>datui-test</Name><Properties/></Container><Container><Name>raw</Name></Container></Containers><NextMarker>/a/raw</NextMarker></EnumerationResults>"#,
        )
        .unwrap();
        assert_eq!(names, ["datui-test", "raw"]);
        assert_eq!(next.as_deref(), Some("/a/raw"));
        let (none, next) = parse_containers(
            "<EnumerationResults><Containers /><NextMarker /></EnumerationResults>",
        )
        .unwrap();
        assert!(none.is_empty() && next.is_none());
    }

    #[test]
    fn a_permission_error_says_how_to_get_permission() {
        let text = describe_error(
            403,
            "<?xml version=\"1.0\"?><Error><Code>AuthorizationPermissionMismatch</Code><Message>This request is not authorized to perform this operation using this permission.\nRequestId:x</Message></Error>",
        );
        assert!(text.starts_with("403 AuthorizationPermissionMismatch: This request"));
        assert!(text.contains("Storage Blob Data Reader"));
        assert!(!text.contains("RequestId"));
    }

    #[test]
    fn folder_markers_are_not_data() {
        let prefixes = vec!["demo/".to_string()];
        assert!(is_folder_marker("demo", 0, &prefixes));
        assert!(!is_folder_marker("demo", 10, &prefixes));
        assert!(!is_folder_marker("empty.csv", 0, &prefixes));
    }
}
