#!/bin/sh
# Installs the latest datui release on Linux or macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh -s -- --user
#
# POSIX sh. Everything runs from main() at the end, so a download cut short
# runs nothing. Questions are read from the terminal (/dev/tty), which is
# still there when the script itself arrives on standard input.
set -eu

REPO="derekwisong/datui"
BINARY_NAME="datui"
APT_REPO="https://derekwisong.github.io/datui-apt"
APT_KEYRING="/usr/share/keyrings/datui-archive-keyring.gpg"
APT_LIST="/etc/apt/sources.list.d/datui.list"
DNF_REPO_FILE="/etc/yum.repos.d/datui.repo"
MIN_GLIBC="2.28"

ASSUME_YES=false
USER_INSTALL=false
VERIFY=true
TMP_DIR=""

usage() {
    cat <<EOF
Install the latest datui release.

Usage: install.sh [OPTION]...

  -y, --yes        Answer yes to every question
      --user       Install into ~/.local/bin (or \$XDG_BIN_HOME) without root; the
                   default where there is no sudo
      --no-verify  Skip checking the download against the release's SHA256SUMS
  -h, --help       Show this and exit

Debian and Ubuntu: adds the datui apt repository and its signing key with sudo, then
installs the package with apt. Fedora and RHEL: adds the datui dnf repository with
sudo, then installs the package with dnf.
Arch on x86_64: installs datui-bin from the AUR with yay or paru, after asking.
Elsewhere, and with --user: unpacks the release archive, the binary and the manual
pages, into /usr/local or your home directory.

Linux needs glibc $MIN_GLIBC or newer (Debian 10, Ubuntu 20.04, RHEL 8 and later).
Uninstall and other methods: https://derekwisong.github.io/datui/latest/getting-started/installation.html
EOF
}

cleanup() {
    # Only a directory this script made, never / or the home directory.
    if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ] && [ "$TMP_DIR" != "/" ] && [ "$TMP_DIR" != "$HOME" ]; then
        rm -rf "$TMP_DIR"
    fi
}

say() {
    printf '%s\n' "$*"
}

fail() {
    printf 'install.sh: %s\n' "$*" >&2
    exit 1
}

# Whether there is a terminal to talk to: /dev/tty exists without one (CI, cron), and
# only opening it tells.
has_tty() {
    (: < /dev/tty) 2> /dev/null
}

# Ask a yes/no question on the terminal. $1 is the question, $2 the answer
# when there is no terminal or -y was given (y or n). Returns 0 for yes.
ask() {
    question="$1"
    default="$2"
    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi
    if ! has_tty; then
        say "No terminal to ask on; taking the default ($default)."
        [ "$default" = y ]
        return
    fi
    printf '%s ' "$question" > /dev/tty
    read -r answer < /dev/tty || answer=""
    case "$answer" in
        [yY]|[yY][eE][sS]) return 0 ;;
        [nN]|[nN][oO]) return 1 ;;
        "") [ "$default" = y ] ;;
        *) return 1 ;;
    esac
}

# Use sudo only when not root (containers often run as root without sudo).
run_priv() {
    if [ "$(id -u)" = 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

fetch() {
    # -f: an HTTP error is a failure, not a saved error page.
    curl -fsSL --retry 3 --retry-delay 2 "$1" -o "$2"
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -y|--yes) ASSUME_YES=true ;;
            --user) USER_INSTALL=true ;;
            --no-verify) VERIFY=false ;;
            -h|--help) usage; exit 0 ;;
            *) say "install.sh: unknown option: $1" >&2; say "" >&2; usage >&2; exit 2 ;;
        esac
        shift
    done
}

detect_system() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    case "$OS" in
        linux) ;;
        darwin) ;;
        *) fail "unsupported system: $OS. Windows: winget install derekwisong.datui" ;;
    esac
    case "$ARCH" in
        x86_64|amd64) ARCH=x86_64 ;;
        aarch64|arm64) ARCH=aarch64 ;;
        *) fail "no build for $OS/$ARCH. Build it with: cargo install datui --locked" ;;
    esac

    if [ "$OS" = linux ]; then
        check_libc
    fi

    if [ "$(id -u)" != 0 ] && ! command -v sudo > /dev/null 2>&1; then
        # Azure Cloud Shell and many shared hosts: the home directory, not /usr/local.
        USER_INSTALL=true
    fi
    USER_BIN_DIR="${XDG_BIN_HOME:-$HOME/.local/bin}"
    USER_MAN_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/man"
}

# The Linux binaries are built against glibc 2.28. musl (Alpine) cannot load
# them, and an older glibc refuses them with a message about GLIBC_2.28.
check_libc() {
    if [ -f /etc/alpine-release ] || ls /lib/ld-musl-*.so.1 > /dev/null 2>&1; then
        fail "this system uses musl, and the Linux builds need glibc $MIN_GLIBC or newer. Build it with: cargo install datui --locked"
    fi
    glibc=$(getconf GNU_LIBC_VERSION 2> /dev/null | awk '{print $2}') || glibc=""
    if [ -n "$glibc" ]; then
        oldest=$(printf '%s\n%s\n' "$MIN_GLIBC" "$glibc" | sort -t. -k1,1n -k2,2n | head -n 1)
        if [ "$oldest" != "$MIN_GLIBC" ]; then
            fail "glibc $glibc found, and the Linux builds need $MIN_GLIBC or newer. Build it with: cargo install datui --locked"
        fi
    fi
}

# Arch and its derivatives (CachyOS, EndeavourOS, Manjaro): /etc/arch-release, or
# arch in os-release's ID or ID_LIKE.
is_arch() {
    [ -f /etc/arch-release ] && return 0
    [ -r /etc/os-release ] || return 1
    # shellcheck disable=SC1091
    ids=$(. /etc/os-release && printf '%s %s' "${ID:-}" "${ID_LIKE:-}")
    case " $ids " in
        *" arch "*) return 0 ;;
    esac
    return 1
}

# Arch on x86_64: datui-bin from the AUR, through yay or paru, so pacman owns it
# and the helper's upgrades keep it current. With neither, or as root (the
# helpers refuse it), say how and offer the archive.
offer_aur() {
    if [ "$OS" != linux ] || [ "$USER_INSTALL" = true ] || ! is_arch; then
        return
    fi
    if [ "$ARCH" != x86_64 ]; then
        say "Arch Linux on $ARCH: datui-bin is x86_64 only; installing the release archive."
        return
    fi
    helper=""
    for h in yay paru; do
        if command -v "$h" > /dev/null 2>&1; then
            helper="$h"
            break
        fi
    done
    if [ -n "$helper" ] && [ "$(id -u)" != 0 ]; then
        if ask "Arch Linux: install datui-bin from the AUR with $helper? [Y/n]" y; then
            install_aur "$helper"
            finish
            exit 0
        fi
    elif [ -n "$helper" ]; then
        say "Arch Linux: $helper does not run as root. As your user: $helper -S datui-bin"
    else
        say "Arch Linux: the AUR has datui-bin (yay -S datui-bin, paru -S datui-bin, or makepkg -si in a clone of https://aur.archlinux.org/datui-bin.git)."
    fi
    if ! ask "Install the release archive into /usr/local instead? [Y/n]" y; then
        say "Stopped. Install it from the AUR."
        exit 0
    fi
}

install_aur() {
    helper="$1"
    if [ -e "/usr/local/bin/$BINARY_NAME" ]; then
        # An earlier archive install there comes first on PATH and would hide the package.
        say "Note: /usr/local/bin/$BINARY_NAME, from an earlier install, comes before /usr/bin on PATH. Remove it: sudo rm /usr/local/bin/$BINARY_NAME"
    fi
    set -- -S --needed datui-bin
    if [ "$ASSUME_YES" = true ] || ! has_tty; then
        set -- "$@" --noconfirm
    fi
    say "Installing datui-bin with $helper..."
    # The helper asks its own questions (sudo, the PKGBUILD); piped into sh, stdin is
    # the script, so they go to the terminal.
    if has_tty; then
        "$helper" "$@" < /dev/tty || fail "$helper -S datui-bin failed"
    else
        "$helper" "$@" || fail "$helper -S datui-bin failed"
    fi
    INSTALLED="/usr/bin/$BINARY_NAME"
}

# The latest release's tag, from where GitHub redirects releases/latest.
resolve_release() {
    final=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest") || final=""
    TAG=${final##*/}
    case "$TAG" in
        v[0-9]*) ;;
        *) fail "could not find the latest release of $REPO (got '$final'). Check the network, or install from https://github.com/$REPO/releases" ;;
    esac
    VERSION="${TAG#v}"
    DOWNLOAD_URL="https://github.com/$REPO/releases/download/$TAG"
    SUMS="$TMP_DIR/SHA256SUMS"
    fetch "$DOWNLOAD_URL/SHA256SUMS" "$SUMS" || fail "release $TAG publishes no SHA256SUMS, so there is nothing to verify a download against. Install it by hand from https://github.com/$REPO/releases/tag/$TAG"
}

# Whether the release has an asset of this name.
has_asset() {
    awk -v n="$1" '$2 == n || $2 == "*" n { found = 1 } END { exit !found }' "$SUMS"
}

# The archive for this machine. Releases from 0.4.0 name it by target triple;
# the names before are kept so the script still installs an older latest.
choose_archive() {
    case "$OS" in
        linux) candidates="datui-$TAG-$ARCH-unknown-linux-gnu.tar.gz datui-$VERSION-$ARCH.tar.gz" ;;
        darwin) candidates="datui-$TAG-$ARCH-apple-darwin.tar.gz" ;;
    esac
    for name in $candidates; do
        if has_asset "$name"; then
            ARCHIVE="$name"
            return
        fi
    done
    fail "release $TAG has no build for $OS/$ARCH. Build it with: cargo install datui --locked"
}

choose_format() {
    if [ "$OS" = darwin ] || [ "$USER_INSTALL" = true ]; then
        FORMAT=tarball
    elif [ -f /etc/debian_version ]; then
        FORMAT=apt
    elif command -v dnf > /dev/null 2>&1; then
        # Fedora, RHEL and its rebuilds, Amazon Linux: the dnf repository.
        FORMAT=rpm
    else
        FORMAT=tarball
    fi
}

# Download an asset into TMP_DIR and check it against SHA256SUMS.
download() {
    name="$1"
    say "Downloading $name..."
    fetch "$DOWNLOAD_URL/$name" "$TMP_DIR/$name" || fail "could not download $DOWNLOAD_URL/$name"
    if [ "$VERIFY" != true ]; then
        say "Not verifying the checksum (--no-verify)."
        return
    fi
    if command -v sha256sum > /dev/null 2>&1; then
        actual=$(sha256sum "$TMP_DIR/$name" | awk '{print $1}')
    elif command -v shasum > /dev/null 2>&1; then
        actual=$(shasum -a 256 "$TMP_DIR/$name" | awk '{print $1}')
    else
        fail "neither sha256sum nor shasum is installed, so the download cannot be verified. Install one, or pass --no-verify"
    fi
    expected=$(awk -v n="$name" '$2 == n || $2 == "*" n { print $1; exit }' "$SUMS")
    [ -n "$expected" ] || fail "$name is not listed in the release's SHA256SUMS, so it cannot be verified. Pass --no-verify to install it anyway"
    if [ "$actual" != "$expected" ]; then
        say "Checksum mismatch for $name." >&2
        say "  expected: $expected" >&2
        say "  actual:   $actual" >&2
        fail "refusing to install. Try again, and if it persists report it at https://github.com/$REPO/security/advisories/new"
    fi
    say "Checksum OK."
}

# Copy the archive's man/manN/ into DIR, or the lone datui.1 of releases before
# 0.4.0. RUN runs each install, through run_priv when the directory needs root.
install_manpages() {
    dir="$1"
    runner="$2"
    if [ -d "$TMP_DIR/man" ]; then
        for section_dir in "$TMP_DIR"/man/man*; do
            [ -d "$section_dir" ] || continue
            section=$(basename "$section_dir")
            $runner install -d "$dir/$section"
            $runner install -m 644 "$section_dir"/* "$dir/$section/"
        done
    elif [ -f "$TMP_DIR/$BINARY_NAME.1" ]; then
        $runner install -d "$dir/man1"
        $runner install -m 644 "$TMP_DIR/$BINARY_NAME.1" "$dir/man1/"
    elif [ -f "$TMP_DIR/target/release/$BINARY_NAME.1.gz" ]; then
        $runner install -d "$dir/man1"
        $runner install -m 644 "$TMP_DIR/target/release/$BINARY_NAME.1.gz" "$dir/man1/"
    fi
}

install_tarball() {
    download "$ARCHIVE"
    tar -xzf "$TMP_DIR/$ARCHIVE" -C "$TMP_DIR"
    [ -f "$TMP_DIR/$BINARY_NAME" ] || fail "$ARCHIVE holds no $BINARY_NAME at its root"

    if [ "$USER_INSTALL" = true ]; then
        say "Installing into $USER_BIN_DIR (no root needed)..."
        install -d "$USER_BIN_DIR"
        install -m 755 "$TMP_DIR/$BINARY_NAME" "$USER_BIN_DIR/$BINARY_NAME"
        install_manpages "$USER_MAN_DIR" ""
        INSTALLED="$USER_BIN_DIR/$BINARY_NAME"
        return
    fi

    say "Installing into /usr/local/bin, with the manual pages in /usr/local/share/man (sudo)..."
    run_priv install -d /usr/local/bin
    run_priv install -m 755 "$TMP_DIR/$BINARY_NAME" "/usr/local/bin/$BINARY_NAME"
    install_manpages /usr/local/share/man run_priv
    INSTALLED="/usr/local/bin/$BINARY_NAME"
}

install_apt() {
    say "Debian/Ubuntu: this adds the datui apt repository and its signing key, with sudo:"
    say "  $APT_KEYRING  (key from $APT_REPO/public.key)"
    say "  $APT_LIST     (deb [signed-by=...] $APT_REPO/ ./)"
    say "then installs the datui package with apt. apt upgrade keeps it current;"
    say "remove those two files and the package to undo it."
    if ! ask "Continue? [Y/n]" y; then
        say "Stopped before changing apt."
        exit 0
    fi
    export DEBIAN_FRONTEND=noninteractive
    run_priv apt-get update -qq || true
    run_priv apt-get install -y --no-install-recommends gnupg ca-certificates curl
    say "Adding the datui apt repository..."
    fetch "$APT_REPO/public.key" "$TMP_DIR/public.key" || fail "could not download $APT_REPO/public.key"
    gpg --dearmor < "$TMP_DIR/public.key" > "$TMP_DIR/keyring.gpg"
    run_priv install -m 644 "$TMP_DIR/keyring.gpg" "$APT_KEYRING"
    say "deb [signed-by=$APT_KEYRING] $APT_REPO/ ./" | run_priv tee "$APT_LIST" > /dev/null
    say "Installing with apt..."
    run_priv apt-get update -qq || true
    run_priv apt-get install -y datui
    INSTALLED="/usr/bin/$BINARY_NAME"
}

apt_with_fallback() {
    if install_apt; then
        return
    fi
    say ""
    say "The apt install failed."
    if ! ask "Install the release archive into /usr/local instead? [Y/n]" y; then
        fail "stopped"
    fi
    install_tarball
}

install_dnf() {
    say "Fedora/RHEL: this adds the datui dnf repository, with sudo:"
    say "  $DNF_REPO_FILE  (from $APT_REPO/rpm/datui.repo)"
    say "then installs the datui package with dnf, importing the repository's signing"
    say "key. dnf upgrade keeps it current; remove that file and the package to undo it."
    if ! ask "Continue? [Y/n]" y; then
        say "Stopped before changing dnf."
        exit 0
    fi
    say "Adding the datui dnf repository..."
    fetch "$APT_REPO/rpm/datui.repo" "$TMP_DIR/datui.repo" || return 1
    run_priv install -m 644 "$TMP_DIR/datui.repo" "$DNF_REPO_FILE" || return 1
    say "Installing with dnf..."
    run_priv dnf install -y datui || return 1
    INSTALLED="/usr/bin/$BINARY_NAME"
}

dnf_with_fallback() {
    if install_dnf; then
        return
    fi
    say ""
    say "The dnf repository install failed."
    # A repository dnf cannot read would fail the .rpm install too.
    if [ -f "$DNF_REPO_FILE" ]; then
        run_priv rm -f "$DNF_REPO_FILE"
    fi
    if ! ask "Install the release's .rpm instead? [Y/n]" y; then
        fail "stopped"
    fi
    install_rpm
}

install_rpm() {
    package="datui-$VERSION-1.$ARCH.rpm"
    has_asset "$package" || fail "release $TAG has no $package. Build it with: cargo install datui --locked"
    download "$package"
    say "Installing with dnf (sudo)..."
    run_priv dnf install -y "$TMP_DIR/$package"
    INSTALLED="/usr/bin/$BINARY_NAME"
}

finish() {
    if ! installed_version=$("$INSTALLED" --version 2>&1); then
        say "$installed_version" >&2
        fail "$INSTALLED is installed but does not run. On Linux it needs glibc $MIN_GLIBC or newer: run 'getconf GNU_LIBC_VERSION'"
    fi
    say ""
    say "--- $installed_version installed at $INSTALLED ---"
    say ""
    if [ "$USER_INSTALL" = true ]; then
        case ":$PATH:" in
            *":$USER_BIN_DIR:"*) ;;
            *)
                say "$USER_BIN_DIR is not on your PATH. Add it in your shell's startup file, for example:"
                say "  export PATH=\"$USER_BIN_DIR:\$PATH\""
                say ""
                ;;
        esac
    fi
    say "Start with: $BINARY_NAME --help"
    say "The manual: man $BINARY_NAME"
}

main() {
    parse_args "$@"
    detect_system
    offer_aur
    TMP_DIR=$(mktemp -d)
    trap cleanup EXIT
    resolve_release
    choose_format
    say "Installing $BINARY_NAME $VERSION for $OS/$ARCH"
    case "$FORMAT" in
        apt) apt_with_fallback ;;
        rpm) dnf_with_fallback ;;
        tarball) choose_archive; install_tarball ;;
    esac
    finish
}

main "$@"
