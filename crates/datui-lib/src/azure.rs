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
            self.auth = AzureAuth::Key(crate::cloud_command::secret(command, env)?);
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
    if let Some(token) = cached_token(&key(scope)) {
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
    if let Some(token) = cached_token(&key) {
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
        .map(|(k, v)| format!("{k}={}", crate::cloud_browse::urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        sp.authority.trim_end_matches('/'),
        crate::cloud_browse::urlencode(&sp.tenant)
    );
    let mut response = crate::cloud_browse::http_agent()
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
    if let Some(token) = cached_token(&key) {
        return Ok(token);
    }
    let resource = crate::cloud_browse::urlencode(scope.trim_end_matches(".default"));
    let client = (env.var)("AZURE_CLIENT_ID")
        .map(|id| format!("&client_id={}", crate::cloud_browse::urlencode(&id)))
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
    let mut response = ureq::Agent::config_builder()
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

fn cached_token(key: &str) -> Option<String> {
    tokens().lock().ok().and_then(|t| {
        t.get(key)
            .filter(|(_, expires)| {
                expires.is_none_or(|at| at > SystemTime::now() + Duration::from_secs(5 * 60))
            })
            .map(|(token, _)| token.clone())
    })
}

fn cache_token(key: &str, token: String, expires: Option<SystemTime>) {
    if let Ok(mut cached) = tokens().lock() {
        cached.insert(key.to_string(), (token, expires));
    }
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
        crate::cloud_browse::urlencode(container),
        crate::cloud_browse::urlencode(&directory)
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
    let text = crate::cloud_browse::http_agent()
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
    let mut response = crate::cloud_browse::http_agent()
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
    Ok(std::sync::Arc::new(build(account, container, settings)?))
}

/// The same store, for one page of a listing at a time.
pub fn paginated_store(
    account: &str,
    container: &str,
    settings: &AzureSettings,
) -> Result<std::sync::Arc<dyn object_store::list::PaginatedListStore>, String> {
    Ok(std::sync::Arc::new(build(account, container, settings)?))
}

fn build(
    account: &str,
    container: &str,
    settings: &AzureSettings,
) -> Result<object_store::azure::MicrosoftAzure, String> {
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
mod tests {
    use super::*;
    use std::path::Path;

    fn env_with<'a>(
        vars: &'a HashMap<&'a str, String>,
        files: &'a HashMap<PathBuf, String>,
        run: &'a crate::cloud_command::Runner<'a>,
        body: impl FnOnce(&Environment<'_>),
    ) {
        let var = |k: &str| vars.get(k).cloned();
        let exists = |p: &Path| files.contains_key(p);
        let read = |p: &Path| files.get(p).cloned();
        let all_vars = Vec::new;
        let list = |_: &Path| Vec::new();
        body(&Environment {
            var: &var,
            exists: &exists,
            read: &read,
            home: Some(PathBuf::from("/home/u")),
            windows: false,
            run,
            all_vars: &all_vars,
            list: &list,
        });
    }

    #[test]
    fn service_principals_from_the_environment() {
        let vars: HashMap<&str, String> = [
            ("AZURE_TENANT_ID", "t"),
            ("AZURE_CLIENT_ID", "c"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
        ]
        .into_iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect();
        let (settings, origin) = from_environment(&|k| vars.get(k).cloned()).unwrap();
        assert_eq!(origin, "AZURE_FEDERATED_TOKEN_FILE");
        assert_eq!(settings.account, None, "finds its accounts");
        assert!(settings.auth.is_identity());
        assert_eq!(settings.auth.describe(), "workload identity");
        // A key beside the account still wins.
        let mut keyed = vars.clone();
        keyed.insert("AZURE_STORAGE_ACCOUNT_NAME", "acct".to_string());
        keyed.insert("AZURE_STORAGE_ACCOUNT_KEY", "a2V5".to_string());
        let (settings, _) = from_environment(&|k| keyed.get(k).cloned()).unwrap();
        assert_eq!(settings.auth, AzureAuth::Key("a2V5".to_string()));
        // No secret, no principal.
        let mut half = vars.clone();
        half.remove("AZURE_FEDERATED_TOKEN_FILE");
        assert!(from_environment(&|k| half.get(k).cloned()).is_none());
        let debug = format!(
            "{:?}",
            ServicePrincipal {
                secret: Some("hunter2".to_string()),
                ..Default::default()
            }
        );
        assert!(!debug.contains("hunter2"), "{debug}");
    }

    #[test]
    fn a_service_principal_token_from_entra_id() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let authority = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = stream.read(&mut buf).unwrap_or(0);
                    request.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&request);
                    if n == 0
                        || text.find("\r\n\r\n").is_some_and(|end| {
                            let length = text
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                                })
                                .unwrap_or(0);
                            request.len() >= end + 4 + length
                        })
                    {
                        break;
                    }
                }
                tx.send(String::from_utf8_lossy(&request).into_owned())
                    .unwrap();
                let body = r#"{"token_type":"Bearer","expires_in":3599,"access_token":"eyJ.sp"}"#;
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        let sp = ServicePrincipal {
            tenant: "tenant-1".to_string(),
            client_id: "client-1".to_string(),
            secret: None,
            token_file: Some(PathBuf::from("/var/run/federated")),
            authority,
        };
        let vars = HashMap::new();
        let files: HashMap<PathBuf, String> = [(
            PathBuf::from("/var/run/federated"),
            "jwt-from-aks\n".to_string(),
        )]
        .into();
        let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
        env_with(&vars, &files, &run, |env| {
            let token = identity_token(
                &AzureAuth::ServicePrincipal(sp.clone()),
                "https://storage.azure.com/.default",
                env,
            )
            .unwrap();
            assert_eq!(token, "eyJ.sp");
        });
        let request = rx.recv().unwrap();
        assert!(
            request.starts_with("POST /tenant-1/oauth2/v2.0/token"),
            "{request}"
        );
        assert!(
            request.contains("client_assertion=jwt-from-aks"),
            "{request}"
        );
        assert!(
            request.contains("grant_type=client_credentials"),
            "{request}"
        );
    }

    #[test]
    fn a_managed_identity_token_from_the_platform() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/msi/token", listener.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                tx.send(String::from_utf8_lossy(&buf[..n]).into_owned())
                    .unwrap();
                let body = r#"{"access_token":"eyJ.mi","expires_on":"4102444800","resource":"https://storage.azure.com/","token_type":"Bearer"}"#;
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        let vars: HashMap<&str, String> = [
            ("IDENTITY_ENDPOINT", endpoint),
            ("IDENTITY_HEADER", "header-secret".to_string()),
        ]
        .into();
        let files = HashMap::new();
        let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
        env_with(&vars, &files, &run, |env| {
            let token = identity_token(
                &AzureAuth::ManagedIdentity,
                "https://management.azure.com/.default",
                env,
            )
            .unwrap();
            assert_eq!(token, "eyJ.mi");
        });
        let request = rx.recv().unwrap().to_ascii_lowercase();
        assert!(request.contains("api-version=2019-08-01"), "{request}");
        assert!(
            request.contains("resource=https%3a%2f%2fmanagement.azure.com%2f"),
            "{request}"
        );
        assert!(
            request.contains("x-identity-header: header-secret"),
            "{request}"
        );
    }

    #[test]
    fn powershell_tokens() {
        let tokens = parse_powershell_tokens(
            r#"{"https://storage.azure.com/":{"token":"st","expires":1893553445},"https://management.azure.com/":{"token":"mg","expires":1893553445}}"#,
        )
        .unwrap();
        assert_eq!(tokens.len(), 2);
        assert!(
            tokens
                .iter()
                .any(|(r, t, _)| r == "https://storage.azure.com/" && t == "st")
        );
        assert!(parse_powershell_tokens("WARNING: something").is_none());
        // The script is a constant with nothing from outside in it.
        assert!(!POWERSHELL_SCRIPT.contains('"'));
    }

    #[test]
    fn powershell_is_asked_when_it_is_the_one_signed_in() {
        let vars = HashMap::new();
        let files: HashMap<PathBuf, String> = [(
            PathBuf::from("/home/u/.Azure/AzureRmContext.json"),
            "{}".to_string(),
        )]
        .into();
        let run = |program: &str, args: &[&str]| match program {
            "az" => Err(CommandError::Failed(
                "Please run 'az login' to setup account.".to_string(),
            )),
            "pwsh" => {
                assert_eq!(&args[..3], ["-NoProfile", "-NonInteractive", "-Command"]);
                Ok(r#"{"https://storage.azure.com/":{"token":"from-pwsh","expires":4102444800},"https://management.azure.com/":{"token":"mg","expires":4102444800}}"#.to_string())
            }
            other => Err(CommandError::Missing(other.to_string())),
        };
        env_with(&vars, &files, &run, |env| {
            assert!(powershell_login_evidence(env));
            let token = identity_token(&AzureAuth::AzCli, STORAGE_SCOPE, env).unwrap();
            assert_eq!(token, "from-pwsh");
        });
    }

    #[test]
    fn not_signed_in_needs_azure_tooling() {
        // Joined with the host's separator, which is what `split_paths` splits on:
        // `:` is one long directory on Windows.
        let path = std::env::join_paths(["/opt/az/bin", "/usr/bin"]).unwrap();
        let vars: HashMap<&str, String> = [
            ("PATH", path.to_string_lossy().into_owned()),
            (
                "PSModulePath",
                "/home/u/.local/share/powershell/Modules".to_string(),
            ),
        ]
        .into();
        let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
        let az_only: HashMap<PathBuf, String> =
            [(PathBuf::from("/opt/az/bin/az"), String::new())].into();
        env_with(&vars, &az_only, &run, |env| {
            assert_eq!(
                not_signed_in(env).as_deref(),
                Some("not signed in: run az login")
            );
        });
        let both: HashMap<PathBuf, String> = [
            (PathBuf::from("/opt/az/bin/az"), String::new()),
            (
                PathBuf::from("/home/u/.local/share/powershell/Modules/Az.Accounts"),
                String::new(),
            ),
        ]
        .into();
        env_with(&vars, &both, &run, |env| {
            assert!(not_signed_in(env).unwrap().contains("Connect-AzAccount"));
        });
        let nothing = HashMap::new();
        env_with(&vars, &nothing, &run, |env| {
            assert_eq!(not_signed_in(env), None)
        });
        let signed_in: HashMap<PathBuf, String> = [
            (PathBuf::from("/opt/az/bin/az"), String::new()),
            (PathBuf::from("/home/u/.azure"), String::new()),
        ]
        .into();
        env_with(&vars, &signed_in, &run, |env| {
            assert_eq!(not_signed_in(env), None)
        });
    }

    #[test]
    fn account_keys_only_after_a_missing_data_role() {
        let vars = HashMap::new();
        let files = HashMap::new();
        let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
        let signed_in = AzureSettings {
            auth: AzureAuth::Bearer("t".to_string()),
            identity: Some(AzureAuth::AzCli),
            ..Default::default()
        };
        let mismatch = "403 AuthorizationPermissionMismatch: This request is not authorized";
        env_with(&vars, &files, &run, |env| {
            // Turned off: the refusal as it was.
            let off = with_account_key("keysoff", &signed_in, mismatch, false, env).unwrap_err();
            assert_eq!(off, mismatch);
            // Another refusal: nothing to get past.
            let other = with_account_key(
                "keysother",
                &signed_in,
                "403 AuthorizationFailure",
                true,
                env,
            )
            .unwrap_err();
            assert_eq!(other, "403 AuthorizationFailure");
            // A key or SAS was refused: no identity to fetch keys with.
            let keyed = AzureSettings {
                auth: AzureAuth::Key("k".to_string()),
                ..Default::default()
            };
            assert_eq!(
                with_account_key("keyskeyed", &keyed, mismatch, true, env).unwrap_err(),
                mismatch
            );
            // The identity cannot fetch keys: the refusal, and why no key was used.
            let failed = with_account_key("keysnoaz", &signed_in, mismatch, true, env).unwrap_err();
            assert!(
                failed.starts_with(mismatch) && failed.contains("needs the Azure CLI"),
                "{failed}"
            );
            // Fetched before: used, with the identity kept.
            remember_key_for_test("keyshad", "a2V5");
            let used = with_account_key("keyshad", &signed_in, mismatch, true, env).unwrap();
            assert_eq!(used.auth, AzureAuth::Key("a2V5".to_string()));
            assert_eq!(used.identity, Some(AzureAuth::AzCli));
        });
        assert_eq!(
            parse_account_id(
                r#"{"data":[{"id":"/subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/a","sharedKey":false}]}"#
            ),
            Some((
                "/subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/a"
                    .to_string(),
                false
            ))
        );
        assert_eq!(parse_account_id(r#"{"data":[]}"#), None);
        assert_eq!(
            parse_keys(r#"{"keys":[{"keyName":"key1","value":"k1","permissions":"FULL"},{"keyName":"key2","value":"k2"}]}"#).as_deref(),
            Some("k1")
        );
    }

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
