#!/bin/sh
set -e

# Configuration
REPO="derekwisong/datui"
BINARY_NAME="datui"
MANPAGE_NAME="${BINARY_NAME}.1"
MANPAGE_GZ_NAME="${MANPAGE_NAME}.gz"
GITHUB_URL="https://github.com/$REPO/releases/latest/download"

# Does the user want to assume yes?
for arg in "$@"; do
  if [ "$arg" = "-y" ] || [ "$arg" = "--yes" ]; then
    ASSUME_YES=true
    break
  fi
done

# When piped (e.g. curl ... | sh), stdin is not a terminal; use -y to avoid
# apt/dnf prompting and aborting the installation.
if [ ! -t 0 ] || [ "$ASSUME_YES" = true ]; then
    NONINTERACTIVE="-y"
else
    NONINTERACTIVE=""
fi

# Use sudo only when not root (e.g. containers often run as root and may not have sudo).
run_priv() {
    if [ "$(id -u)" = 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

# Identify System
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Normalize Architecture
case "$ARCH" in
    x86_64)  CANONICAL_ARCH="amd64" ;;
    aarch64|arm64) CANONICAL_ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# If Arch user in an interactive terminal, prompt to install via AUR instead.
# When stdin is not a TTY (e.g. piped or CI), skip the prompt and proceed with binary install.
if [ -f /etc/arch-release ] && [ "$ASSUME_YES" != true ] && [ -t 0 ]; then
    echo "-------------------------------------------------------"
    echo " ARCH LINUX DETECTED"
    echo "-------------------------------------------------------"
    echo "You may install via the AUR instead if desired:"
    echo "  paru -S datui-bin  (or your preferred AUR helper)"
    echo ""
    
    printf "Would you like to continue with the direct binary install anyway? [y/N]: "
    read -r response
    case "$response" in
        [yY][eE][sS]|[yY]) 
            echo "Proceeding with binary installation..."
            ;;
        *)
            echo "Installation cancelled. Please use the AUR package."
            exit 0
            ;;
    esac
fi

# Determine latest version tag from GitHub (used for rpm/tarball installs)
TAG=$(curl -sI https://github.com/$REPO/releases/latest | grep -i "location:" | awk -F/ '{print $NF}' | tr -d '\r\n')
VERSION="${TAG#v}"

# Detect Package Manager / Format
case "$OS" in
    linux*)
        if [ -f /etc/debian_version ]; then
            # on ubuntu/debian-based systems use the APT repository
            FORMAT="apt"
        elif [ -f /etc/redhat-release ] || [ -f /etc/fedora-release ]; then
            # on redhat/rpm-based systems use the rpm package
            FORMAT="rpm"
            FILENAME="${BINARY_NAME}-${VERSION}-1.${ARCH}.rpm"
        else
            # on other systems use the binary tarball
            FORMAT="tar.gz"
            FILENAME="${BINARY_NAME}-${VERSION}-${ARCH}.tar.gz"
        fi
        ;;
    darwin*)
        # macOS: release tarballs use target triple (datui-v0.2.32-aarch64-apple-darwin.tar.gz)
        FORMAT="tar.gz"
        case "$ARCH" in
            arm64|aarch64) MACOS_TARGET="aarch64-apple-darwin" ;;
            x86_64)        MACOS_TARGET="x86_64-apple-darwin" ;;
            *) echo "Unsupported macOS architecture: $ARCH"; exit 1 ;;
        esac
        FILENAME="${BINARY_NAME}-${TAG}-${MACOS_TARGET}.tar.gz"
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

# Download (skip when using APT repository)
if [ "$FORMAT" != "apt" ]; then
    echo "Installing $BINARY_NAME $VERSION for $OS ($CANONICAL_ARCH)"
    TMP_DIR=$(mktemp -d)
    echo "Downloading $FILENAME... ($TMP_DIR)"
    curl -sSL "$GITHUB_URL/$FILENAME" -o "$TMP_DIR/$FILENAME"
fi

# Install based on format
case "$FORMAT" in
    apt)
        echo "Installing $BINARY_NAME for $OS ($CANONICAL_ARCH) via APT repository"
        echo "Ensuring gnupg is installed..."
        run_priv apt-get update -qq && run_priv apt-get install $NONINTERACTIVE --no-install-recommends gnupg
        echo "Adding Datui APT repository..."
        curl -fsSL https://derekwisong.github.io/datui-apt/public.key | gpg --dearmor | run_priv tee /usr/share/keyrings/datui-archive-keyring.gpg > /dev/null
        echo "deb [signed-by=/usr/share/keyrings/datui-archive-keyring.gpg] https://derekwisong.github.io/datui-apt/ ./" | run_priv tee /etc/apt/sources.list.d/datui.list > /dev/null
        echo "Installing via apt..."
        run_priv apt-get update -qq && run_priv apt-get install $NONINTERACTIVE datui
        ;;
    rpm)
        echo "Installing via dnf..."
        run_priv dnf install $NONINTERACTIVE "$TMP_DIR/$FILENAME"
        ;;
    tar.gz)
        echo "Extracting binary to /usr/local/bin..."
        tar -xzf "$TMP_DIR/$FILENAME" -C "$TMP_DIR"

        if [ -f "$TMP_DIR/$MANPAGE_NAME" ]; then
            # macOS tarball has datui/datui.1 at root
            MANPAGE_PATH="$TMP_DIR/$MANPAGE_NAME"
        else
            # Linux tarball has target/release/datui.1.gz
            MANPAGE_PATH="$TMP_DIR/target/release/$MANPAGE_GZ_NAME"
        fi

        run_priv install -d /usr/local/bin
        run_priv install -m 755 "$TMP_DIR/$BINARY_NAME" "/usr/local/bin/$BINARY_NAME"
        run_priv install -d /usr/local/share/man/man1
        run_priv install -m 644 "$MANPAGE_PATH" "/usr/local/share/man/man1/"
        ;;
esac

# Cleanup
if [ -n "${TMP_DIR:-}" ]; then
    rm -rf "$TMP_DIR"
fi

echo ""
echo "--- $($BINARY_NAME --version) installed successfully! ---"
echo ""
echo "For instructions, see: $BINARY_NAME --help"
# if linux or macos, suggest the man page
if [ "$OS" = "linux" ] || [ "$OS" = "macos" ]; then
    echo "For the manpage, run: man $BINARY_NAME"
fi
