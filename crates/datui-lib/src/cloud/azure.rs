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

use crate::cloud::cloud_browse::Environment;
use crate::cloud::cloud_command::CommandError;
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
    /// A signed-in `az`, asked for a token when one is needed. When `az` is not signed
    /// in and Azure PowerShell is, PowerShell is asked instead.
    AzCli,
    /// A signed-in Azure PowerShell (`Connect-AzAccount`).
    PowerShell,
    /// A service principal or workload identity from `AZURE_CLIENT_ID` and friends.
    ServicePrincipal(ServicePrincipal),
    /// The managed identity of the VM, App Service or Function datui runs on.
    ManagedIdentity,
    /// The account key, printed by a `secret_command`.
    KeyCommand(String),
    /// An Entra ID token.
    Bearer(String),
    /// The account's shared key.
    Key(String),
    /// A shared access signature, without the leading `?`.
    Sas(String),
}

/// An application's identity in Entra ID: a client secret, or a federated token file
/// (AKS workload identity) exchanged for a token each time.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct ServicePrincipal {
    pub tenant: String,
    pub client_id: String,
    pub secret: Option<String>,
    pub token_file: Option<PathBuf>,
    /// `AZURE_AUTHORITY_HOST`, for sovereign clouds.
    pub authority: String,
}

impl std::fmt::Debug for ServicePrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServicePrincipal")
            .field("tenant", &self.tenant)
            .field("client_id", &self.client_id)
            .field("secret", &self.secret.as_ref().map(|_| "…"))
            .field("token_file", &self.token_file)
            .finish()
    }
}

impl AzureAuth {
    /// A login that is asked for tokens: it may reach any account it can see.
    pub fn is_identity(&self) -> bool {
        matches!(
            self,
            AzureAuth::AzCli
                | AzureAuth::PowerShell
                | AzureAuth::ServicePrincipal(_)
                | AzureAuth::ManagedIdentity
        )
    }

    /// Where the login is from, in a word or two.
    pub fn describe(&self) -> &'static str {
        match self {
            AzureAuth::None => "none",
            AzureAuth::AzCli => "az login",
            AzureAuth::PowerShell => "Azure PowerShell",
            AzureAuth::ServicePrincipal(sp) if sp.token_file.is_some() => "workload identity",
            AzureAuth::ServicePrincipal(_) => "service principal",
            AzureAuth::ManagedIdentity => "managed identity",
            AzureAuth::KeyCommand(_) => "secret_command",
            AzureAuth::Bearer(_) => "token",
            AzureAuth::Key(_) => "access key",
            AzureAuth::Sas(_) => "SAS token",
        }
    }
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
    /// The login a token in `auth` came from, kept for what needs a token of another
    /// scope: finding accounts, fetching an account's keys.
    pub identity: Option<AzureAuth>,
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

    /// These settings with a token in place of an identity. Runs `az` or PowerShell, or
    /// asks Entra ID.
    pub fn with_token(mut self, env: &Environment<'_>) -> Result<Self, String> {
        if let AzureAuth::KeyCommand(command) = &self.auth {
            self.auth = AzureAuth::Key(crate::cloud::cloud_command::secret(command, env)?);
        }
        if self.auth.is_identity() {
            let token = identity_token(&self.auth, STORAGE_SCOPE, env)?;
            self.identity = Some(std::mem::replace(&mut self.auth, AzureAuth::Bearer(token)));
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
            identity: None,
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
        identity: None,
    })
}

/// What the environment says about Azure, if anything: a connection string, an account
/// with a key or SAS token, or a service principal (with an account, or to find them).
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
    let account = set("AZURE_STORAGE_ACCOUNT_NAME");
    let key = set("AZURE_STORAGE_ACCOUNT_KEY").or_else(|| set("AZURE_STORAGE_ACCESS_KEY"));
    let sas = set("AZURE_STORAGE_SAS_TOKEN").or_else(|| set("AZURE_STORAGE_SAS_KEY"));
    let (auth, origin) = match (&account, key, sas, service_principal(var)) {
        (Some(_), Some(key), _, _) => (AzureAuth::Key(key), "AZURE_STORAGE_ACCOUNT_KEY"),
        (Some(_), None, Some(sas), _) => (
            AzureAuth::Sas(sas.trim_start_matches('?').to_string()),
            "AZURE_STORAGE_SAS_TOKEN",
        ),
        (_, _, _, Some(sp)) => {
            let origin = if sp.token_file.is_some() {
                "AZURE_FEDERATED_TOKEN_FILE"
            } else {
                "AZURE_CLIENT_SECRET"
            };
            (AzureAuth::ServicePrincipal(sp), origin)
        }
        _ => return None,
    };
    Some((
        AzureSettings {
            account,
            auth,
            blob_endpoint: None,
            use_emulator: false,
            identity: None,
        },
        origin.to_string(),
    ))
}

/// A service principal from `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and either
/// `AZURE_CLIENT_SECRET` or `AZURE_FEDERATED_TOKEN_FILE`, the variables the Azure SDKs
/// and AKS workload identity set.
pub fn service_principal(var: &dyn Fn(&str) -> Option<String>) -> Option<ServicePrincipal> {
    let set = |key: &str| {
        var(key)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let tenant = set("AZURE_TENANT_ID")?;
    let client_id = set("AZURE_CLIENT_ID")?;
    let secret = set("AZURE_CLIENT_SECRET");
    let token_file = set("AZURE_FEDERATED_TOKEN_FILE").map(PathBuf::from);
    if secret.is_none() && token_file.is_none() {
        return None;
    }
    Some(ServicePrincipal {
        tenant,
        client_id,
        secret,
        token_file,
        authority: set("AZURE_AUTHORITY_HOST")
            .unwrap_or_else(|| "https://login.microsoftonline.com".to_string()),
    })
}

/// Whether `az` has been used on this machine: its config directory exists. Not proof the
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

/// Whether Azure PowerShell has been signed in on this machine: its context file exists.
pub fn powershell_login_evidence(env: &Environment<'_>) -> bool {
    powershell_context(env).is_some_and(|path| (env.exists)(&path))
}

fn powershell_context(env: &Environment<'_>) -> Option<PathBuf> {
    // `~/.Azure` on every platform; on Windows it is the same directory as `az`'s
    // `.azure`.
    Some(
        env.home
            .as_ref()?
            .join(".Azure")
            .join("AzureRmContext.json"),
    )
}

/// Azure tooling on this machine with no sign-in to show for it: `az` on `PATH`, or the
/// Az.Accounts PowerShell module installed. The fix, naming what is there, when so.
pub fn not_signed_in(env: &Environment<'_>) -> Option<String> {
    if az_login_evidence(env) || powershell_login_evidence(env) {
        return None;
    }
    let on_path = |names: &[&str]| {
        (env.var)("PATH").is_some_and(|path| {
            std::env::split_paths(&path)
                .any(|dir| names.iter().any(|name| (env.exists)(&dir.join(name))))
        })
    };
    let az = if env.windows {
        on_path(&["az.cmd", "az.exe"])
    } else {
        on_path(&["az"])
    };
    let az_accounts = (env.var)("PSModulePath").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| (env.exists)(&dir.join("Az.Accounts")))
    });
    match (az, az_accounts) {
        (false, false) => None,
        (true, false) => Some("not signed in: run az login".to_string()),
        (false, true) => Some("not signed in: run Connect-AzAccount in PowerShell".to_string()),
        (true, true) => {
            Some("not signed in: run az login, or Connect-AzAccount in PowerShell".to_string())
        }
    }
}

/// A token for `scope` from an identity: `az`, Azure PowerShell or a service principal.
pub fn identity_token(
    auth: &AzureAuth,
    scope: &str,
    env: &Environment<'_>,
) -> Result<String, String> {
    match auth {
        AzureAuth::AzCli => match token(scope, env) {
            Ok(token) => Ok(token),
            // `az` is preferred, since it starts faster; PowerShell is asked when it is
            // the one signed in.
            Err(az) if powershell_login_evidence(env) => {
                powershell_token(scope, env).map_err(|ps| format!("{az}; {ps}"))
            }
            Err(e) => Err(e),
        },
        AzureAuth::PowerShell => powershell_token(scope, env),
        AzureAuth::ServicePrincipal(sp) => service_principal_token(sp, scope, env),
        AzureAuth::ManagedIdentity => managed_identity_token(scope, env),
        AzureAuth::Bearer(token) => Ok(token.clone()),
        AzureAuth::None | AzureAuth::Key(_) | AzureAuth::Sas(_) | AzureAuth::KeyCommand(_) => {
            Err("this login has no tokens".to_string())
        }
    }
}

/// Tokens by scope or login, until shortly before they expire.
fn tokens() -> &'static crate::cloud::cloud_command::Expiring<String> {
    static TOKENS: OnceLock<crate::cloud::cloud_command::Expiring<String>> = OnceLock::new();
    TOKENS.get_or_init(Default::default)
}

/// A token for `scope` from the signed-in `az`.
pub fn token(scope: &str, env: &Environment<'_>) -> Result<String, String> {
    if let Some(token) = tokens().get(scope) {
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
    cache_token(scope, token.clone(), expires);
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

/// The script Azure PowerShell runs: both scopes' tokens and expiries as JSON. A
/// constant, with nothing interpolated into it. The token is a `SecureString` in recent
/// Az.Accounts, and `NetworkCredential` turns it back into text in PowerShell 5.1 and 7
/// alike.
const POWERSHELL_SCRIPT: &str = "$ErrorActionPreference = 'Stop'; \
Import-Module Az.Accounts; \
$out = @{}; \
foreach ($r in @('https://storage.azure.com/', 'https://management.azure.com/')) { \
  $t = Get-AzAccessToken -ResourceUrl $r; \
  $tok = $t.Token; \
  if ($tok -is [System.Security.SecureString]) { $tok = [System.Net.NetworkCredential]::new('', $tok).Password }; \
  $out[$r] = @{ token = $tok; expires = $t.ExpiresOn.ToUnixTimeSeconds() } \
}; \
$out | ConvertTo-Json -Compress";

/// A token for `scope` from Azure PowerShell. One process fetches both scopes, and both
/// are cached.
fn powershell_token(scope: &str, env: &Environment<'_>) -> Result<String, String> {
    let key = |scope: &str| format!("powershell {scope}");
    if let Some(token) = tokens().get(&key(scope)) {
        return Ok(token);
    }
    let args = [
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        POWERSHELL_SCRIPT,
    ];
    let mut output = (env.run)("pwsh", &args);
    if env.windows && matches!(output, Err(CommandError::Missing(_))) {
        output = (env.run)("powershell", &args);
    }
    let output = output.map_err(|e| match e {
        CommandError::Missing(_) => "needs PowerShell".to_string(),
        CommandError::Failed(message) if message.contains("Connect-AzAccount") => {
            format!("not signed in: run Connect-AzAccount ({})", message.trim())
        }
        CommandError::Failed(message) if message.contains("Az.Accounts") => {
            "needs the Az.Accounts PowerShell module".to_string()
        }
        other => other.to_string(),
    })?;
    let tokens = parse_powershell_tokens(&output)
        .ok_or_else(|| "Azure PowerShell returned no token".to_string())?;
    let mut wanted = None;
    for (resource, token, expires) in tokens {
        let scope_of = format!("{resource}.default");
        if scope_of == scope {
            wanted = Some(token.clone());
        }
        cache_token(&key(&scope_of), token, Some(expires));
    }
    wanted.ok_or_else(|| format!("Azure PowerShell returned no token for {scope}"))
}

/// `(resource, token, expiry)` for each scope in the PowerShell script's output.
pub fn parse_powershell_tokens(text: &str) -> Option<Vec<(String, String, SystemTime)>> {
    let value: serde_json::Value = serde_json::from_str(text.trim()).ok()?;
    let tokens: Vec<_> = value
        .as_object()?
        .iter()
        .filter_map(|(resource, entry)| {
            let token = entry.get("token")?.as_str()?.to_string();
            let expires = entry.get("expires")?.as_u64()?;
            (!token.is_empty()).then(|| {
                (
                    resource.clone(),
                    token,
                    SystemTime::UNIX_EPOCH + Duration::from_secs(expires),
                )
            })
        })
        .collect();
    (!tokens.is_empty()).then_some(tokens)
}

/// A token for `scope` for a service principal, from Entra ID's token endpoint: a client
/// secret, or the federated token file read afresh, since AKS rotates it.
fn service_principal_token(
    sp: &ServicePrincipal,
    scope: &str,
    env: &Environment<'_>,
) -> Result<String, String> {
    let key = format!("sp {} {} {scope}", sp.tenant, sp.client_id);
    if let Some(token) = tokens().get(&key) {
        return Ok(token);
    }
    let mut form: Vec<(&str, String)> = vec![
        ("client_id", sp.client_id.clone()),
        ("scope", scope.to_string()),
        ("grant_type", "client_credentials".to_string()),
    ];
    match (&sp.token_file, &sp.secret) {
        (Some(file), _) => {
            let assertion = (env.read)(file).ok_or_else(|| {
                format!("cannot read AZURE_FEDERATED_TOKEN_FILE {}", file.display())
            })?;
            form.push((
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".to_string(),
            ));
            form.push(("client_assertion", assertion.trim().to_string()));
        }
        (None, Some(secret)) => form.push(("client_secret", secret.clone())),
        (None, None) => return Err("the service principal has no secret".to_string()),
    }
    let body = form
        .iter()
        .map(|(k, v)| format!("{k}={}", crate::cloud::cloud_browse::urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        sp.authority.trim_end_matches('/'),
        crate::cloud::cloud_browse::urlencode(&sp.tenant)
    );
    let mut response = crate::cloud::cloud_browse::http_agent()
        .post(&url)
        .config()
        .http_status_as_error(false)
        .build()
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send(body)
        .map_err(|e| format!("{e}"))?;
    let status = response.status().as_u16();
    let text = response
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("could not read the response: {e}"))?;
    let (token, expires) = parse_entra_token(&text).ok_or_else(|| {
        let value: Option<serde_json::Value> = serde_json::from_str(&text).ok();
        let description = value
            .as_ref()
            .and_then(|v| v.get("error_description"))
            .and_then(|d| d.as_str())
            .and_then(|d| d.lines().next())
            .unwrap_or("no token in the response");
        format!("not logged in: the service principal was refused ({status}): {description}")
    })?;
    cache_token(&key, token.clone(), Some(expires));
    Ok(token)
}

/// A token for `scope` from the platform's managed identity: App Service and Functions'
/// `IDENTITY_ENDPOINT` (or the older `MSI_ENDPOINT`), else the VM's instance metadata
/// service. Only reached when the platform or `instance_identity` allows it.
fn managed_identity_token(scope: &str, env: &Environment<'_>) -> Result<String, String> {
    let key = format!("managed {scope}");
    if let Some(token) = tokens().get(&key) {
        return Ok(token);
    }
    let resource = crate::cloud::cloud_browse::urlencode(scope.trim_end_matches(".default"));
    let client = (env.var)("AZURE_CLIENT_ID")
        .map(|id| format!("&client_id={}", crate::cloud::cloud_browse::urlencode(&id)))
        .unwrap_or_default();
    let set = |k: &str| (env.var)(k).filter(|v| !v.trim().is_empty());
    let (url, header) = if let (Some(endpoint), Some(secret)) =
        (set("IDENTITY_ENDPOINT"), set("IDENTITY_HEADER"))
    {
        (
            format!("{endpoint}?api-version=2019-08-01&resource={resource}{client}"),
            ("X-IDENTITY-HEADER", secret),
        )
    } else if let Some(endpoint) = set("MSI_ENDPOINT") {
        // App Service's older endpoint takes a secret; Cloud Shell's takes none and
        // wants the metadata header instead.
        (
            format!("{endpoint}?api-version=2017-09-01&resource={resource}{client}"),
            match set("MSI_SECRET") {
                Some(secret) => ("secret", secret),
                None => ("Metadata", "true".to_string()),
            },
        )
    } else {
        (
            format!(
                "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={resource}{client}"
            ),
            ("Metadata", "true".to_string()),
        )
    };
    let mut response = crate::cloud::user_agent::ureq_config()
        .timeout_global(Some(Duration::from_secs(5)))
        .http_status_as_error(false)
        .build()
        .new_agent()
        .get(&url)
        .header(header.0, &header.1)
        .call()
        .map_err(|e| format!("no managed identity answered: {e}"))?;
    let status = response.status().as_u16();
    let text = response
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("could not read the response: {e}"))?;
    let (token, expires) = parse_entra_token(&text)
        .ok_or_else(|| format!("the managed identity returned no token ({status})"))?;
    cache_token(&key, token.clone(), Some(expires));
    Ok(token)
}

/// The access token and expiry from an Entra ID token response.
pub fn parse_entra_token(text: &str) -> Option<(String, SystemTime)> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let token = value.get("access_token")?.as_str()?.to_string();
    let number = |key: &str| {
        value
            .get(key)
            .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
    };
    // Managed identity endpoints give `expires_on` in seconds since the epoch.
    let expires = match (number("expires_on"), number("expires_in")) {
        (Some(on), _) => SystemTime::UNIX_EPOCH + Duration::from_secs(on),
        (None, Some(within)) => SystemTime::now() + Duration::from_secs(within),
        (None, None) => SystemTime::now() + Duration::from_secs(300),
    };
    (!token.is_empty()).then_some((token, expires))
}

fn cache_token(key: &str, token: String, expires: Option<SystemTime>) {
    crate::logging::keep_out_of_log(&token);
    tokens().put(key, token, expires);
}

/// Account keys fetched after a 403, by account. In memory only: never cached to disk,
/// logged or shown.
fn account_keys() -> &'static Mutex<HashMap<String, String>> {
    static KEYS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    KEYS.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) fn remember_key_for_test(account: &str, key: &str) {
    if let Ok(mut keys) = account_keys().lock() {
        keys.insert(account.to_string(), key.to_string());
    }
}

/// The key this session reads `account` with, when a token was refused and its keys
/// were fetched.
pub fn remembered_key(account: &str) -> Option<String> {
    account_keys().lock().ok()?.get(account).cloned()
}

fn token_readable_accounts() -> &'static Mutex<std::collections::HashSet<String>> {
    static ACCOUNTS: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    ACCOUNTS.get_or_init(Default::default)
}

/// Whether a sign-in's token has read from `account` this session.
pub fn token_reads(account: &str) -> bool {
    token_readable_accounts()
        .lock()
        .is_ok_and(|accounts| accounts.contains(account))
}

/// Note that a sign-in's token read from `account`.
pub fn remember_token_reads(account: &str) {
    if let Ok(mut accounts) = token_readable_accounts().lock() {
        accounts.insert(account.to_string());
    }
}

/// Whether `settings` may read at `path` in `container`: one listing of at most one
/// blob, which needs the same data permission a read does.
pub fn check_read(
    account: &str,
    container: &str,
    path: &str,
    settings: &AzureSettings,
) -> Result<(), String> {
    let directory = match path.trim_start_matches('/').rsplit_once('/') {
        Some((directory, _)) => format!("{directory}/"),
        None => String::new(),
    };
    let url = format!(
        "{}{}?restype=container&comp=list&maxresults=1&prefix={}",
        settings.blob_endpoint_for(account),
        crate::cloud::cloud_browse::urlencode(container),
        crate::cloud::cloud_browse::urlencode(&directory)
    );
    send_signed(&url, account, settings).map(|_| ())
}

/// Whether a refusal is the one account keys get past: a sign-in with no data role.
pub fn is_permission_mismatch(error: &str) -> bool {
    error.contains("AuthorizationPermissionMismatch")
}

/// The first access key of `account`, as the Portal fetches them for someone with Owner
/// or Contributor and no data role: Resource Graph for the account's ID and whether it
/// allows shared keys, then `listKeys` with a management token from `identity`.
pub fn fetch_account_key(
    account: &str,
    identity: &AzureAuth,
    env: &Environment<'_>,
) -> Result<String, String> {
    if let Some(key) = remembered_key(account) {
        return Ok(key);
    }
    let management = identity_token(identity, MANAGEMENT_SCOPE, env)?;
    let query = format!(
        "resources | where type =~ 'microsoft.storage/storageaccounts' and name =~ '{}' \
         | project id, name, sharedKey=properties.allowSharedKeyAccess",
        account.replace('\'', "")
    );
    let body = serde_json::json!({ "query": query });
    let text = crate::cloud::cloud_browse::http_agent()
        .post("https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01")
        .header("Authorization", &format!("Bearer {management}"))
        .header("Content-Type", "application/json")
        .send(body.to_string())
        .map_err(|e| format!("{e}"))?
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("could not read the response: {e}"))?;
    let (id, shared_key) = parse_account_id(&text)
        .ok_or_else(|| format!("{account} is not an account this login can manage"))?;
    if !shared_key {
        return Err("shared-key access is disabled on the account".to_string());
    }
    let mut response = crate::cloud::cloud_browse::http_agent()
        .post(&format!(
            "https://management.azure.com{id}/listKeys?api-version=2023-01-01"
        ))
        .config()
        .http_status_as_error(false)
        .build()
        .header("Authorization", &format!("Bearer {management}"))
        .header("Content-Length", "0")
        .send_empty()
        .map_err(|e| format!("{e}"))?;
    let status = response.status().as_u16();
    let text = response
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("could not read the response: {e}"))?;
    if status != 200 {
        return Err(format!(
            "the login may not list the account's keys ({status})"
        ));
    }
    let key = parse_keys(&text).ok_or_else(|| "the account returned no keys".to_string())?;
    crate::logging::keep_out_of_log(&key);
    if let Ok(mut keys) = account_keys().lock() {
        keys.insert(account.to_string(), key.clone());
    }
    Ok(key)
}

/// The resource ID and shared-key setting of the one account a Resource Graph query
/// found.
pub fn parse_account_id(text: &str) -> Option<(String, bool)> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let row = value.get("data")?.as_array()?.first()?;
    let id = row.get("id")?.as_str()?.to_string();
    let shared_key = row.get("sharedKey").and_then(|v| v.as_bool()) != Some(false);
    Some((id, shared_key))
}

/// The first key from a `listKeys` response.
pub fn parse_keys(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    value
        .get("keys")?
        .as_array()?
        .iter()
        .find_map(|k| k.get("value")?.as_str().map(str::to_string))
        .filter(|k| !k.is_empty())
}

/// After a 403 on a sign-in's token: the same settings with the account's key, when
/// the fallback is on and the account allows it, else the refusal with the reason.
pub fn with_account_key(
    account: &str,
    settings: &AzureSettings,
    refusal: &str,
    enabled: bool,
    env: &Environment<'_>,
) -> Result<AzureSettings, String> {
    let identity = match &settings.identity {
        Some(identity) if enabled && is_permission_mismatch(refusal) => identity,
        _ => return Err(refusal.to_string()),
    };
    match fetch_account_key(account, identity, env) {
        Ok(key) => Ok(AzureSettings {
            auth: AzureAuth::Key(key),
            ..settings.clone()
        }),
        Err(why) => Err(format!(
            "{refusal}. The account's keys were not used: {why}"
        )),
    }
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
pub fn discover_accounts(
    identity: &AzureAuth,
    env: &Environment<'_>,
) -> Result<Vec<Account>, String> {
    let token = identity_token(identity, MANAGEMENT_SCOPE, env)?;
    crate::cloud::cloud_command::paged(MAX_ACCOUNT_PAGES, |skip| {
        let mut options = serde_json::json!({ "$top": 1000 });
        if let Some(skip) = skip {
            options["$skipToken"] = serde_json::Value::String(skip.to_string());
        }
        let body = serde_json::json!({ "query": ACCOUNTS_QUERY, "options": options });
        let text = crate::cloud::cloud_browse::http_agent()
            .post("https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01")
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .send(body.to_string())
            .map_err(|e| format!("{e}"))?
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("could not read the response: {e}"))?;
        parse_accounts(&text)
    })
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
    let mut names = crate::cloud::cloud_command::paged(MAX_ACCOUNT_PAGES, |marker| {
        let mut url = format!("{endpoint}?comp=list&maxresults=5000");
        if let Some(marker) = marker {
            url.push_str(&format!(
                "&marker={}",
                crate::cloud::cloud_browse::urlencode(marker)
            ));
        }
        parse_containers(&send_signed(&url, account, settings)?)
    })?;
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
        // No `x-ms-date` of datui's own: the authorizer adds `Date` and signs it, and a
        // second date header makes a shared-key signature one the service rejects.
        .header("x-ms-version", API_VERSION)
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
        AzureAuth::Sas(_)
        | AzureAuth::None
        | AzureAuth::AzCli
        | AzureAuth::PowerShell
        | AzureAuth::ServicePrincipal(_)
        | AzureAuth::ManagedIdentity
        | AzureAuth::KeyCommand(_) => {}
    }
    let mut call = crate::cloud::cloud_browse::http_agent()
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
) -> Result<object_store::azure::MicrosoftAzure, String> {
    let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container)
        .with_config(
            object_store::azure::AzureConfigKey::Client(crate::cloud::user_agent::CLIENT_KEY),
            crate::cloud::user_agent::get(),
        );
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
        AzureAuth::None
        | AzureAuth::AzCli
        | AzureAuth::PowerShell
        | AzureAuth::ServicePrincipal(_)
        | AzureAuth::ManagedIdentity
        | AzureAuth::KeyCommand(_) => builder.with_skip_signature(true),
    };
    builder
        .build()
        .map_err(|e| format!("Azure is not configured: {e}"))
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
        AzureAuth::None
        | AzureAuth::AzCli
        | AzureAuth::PowerShell
        | AzureAuth::ServicePrincipal(_)
        | AzureAuth::ManagedIdentity
        | AzureAuth::KeyCommand(_) => {
            options.push((AzureConfigKey::SkipSignature, "true".to_string()))
        }
    }
    options
}

/// Whether a listed object is only a folder marker. Accounts with hierarchical
/// namespace list every directory twice, once as a prefix and once as an empty blob of
/// the same name; the blob is not data.
pub fn is_folder_marker(name: &str, size: u64, prefixes: &[String]) -> bool {
    size == 0 && prefixes.iter().any(|p| p.trim_end_matches('/') == name)
}

#[cfg(test)]
mod tests;
